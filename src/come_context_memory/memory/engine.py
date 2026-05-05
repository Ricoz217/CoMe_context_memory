from __future__ import annotations

import json
import asyncio
import hashlib
import random
import shutil
import yaml
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator
from uuid import uuid4

from .llm_pipeline import LLMPipelineV3
from .aliasing import AliasCodec, AliasPayloadError, stable_payload_hash
from .memory_manager import MemoryManager
from .services import (
    BucketSummaryService,
    BucketTopologyService,
    CompressSplitService,
    IngestService,
    MaintenanceService,
    OptimizeService,
    QueryService,
    ServiceRuntime,
    SplitIngestJobService,
)
from .models import (
    BUCKET_KIND_BUCKET,
    BUCKET_KIND_MEMORY,
    AddResult,
    BucketInfo,
    CleanupResult,
    CompressResult,
    DeleteResult,
    EngineStats,
    GCResult,
    MemoryRecord,
    MoveResult,
    OptimizeResult,
    QueryMatch,
    QueryResult,
    UpdateResult,
    normalize_relations,
    parse_iso_or_none,
    utc_now_iso,
)
from .multimodal import ImageTextExtractor
from .rerank import BM25IndexCache, louvain_split_groups
from .storage import MemoryStorageV3

from come_context_memory.LLM_usage import LLMUsage
from come_context_memory.utils import AutoMapping, atomic_save_json


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


class _UnconfiguredStorageProxy:
    def __getattr__(self, name: str) -> Any:
        raise RuntimeError(
            "memory base_dir is not configured; call ContextMemorySystem.get_instance(config=...) "
            "or create ContextMemoryEngineV3 with ContextMemoryConfig(base_dir=...)."
        )


class BucketLockManager:
    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()

    async def _get_lock(self, bucket_id: str) -> asyncio.Lock:
        token = str(bucket_id or "").strip() or "__empty_bucket__"
        async with self._guard:
            lock = self._locks.get(token)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[token] = lock
            return lock

    @asynccontextmanager
    async def acquire_many(self, bucket_ids: list[str] | tuple[str, ...] | set[str]):
        ordered = sorted({str(x or "").strip() for x in bucket_ids if str(x or "").strip()})
        locks: list[asyncio.Lock] = []
        for bucket_id in ordered:
            locks.append(await self._get_lock(bucket_id))
        for lock in locks:
            await lock.acquire()
        try:
            yield
        finally:
            for lock in reversed(locks):
                lock.release()


TOOL_PRESET_KEYS: tuple[str, ...] = (
    "clean",
    "ingest",
    "query",
    "compress",
    "bucket_split",
    "text_chunk",
    "bucket_summary",
    "optimize",
    "image_extract",
)


def _normalize_tool_presets(tool_presets: dict[str, str] | None) -> dict[str, str]:
    if not isinstance(tool_presets, dict):
        return {}
    normalized: dict[str, str] = {}
    for k, v in tool_presets.items():
        key = str(k).strip().lower()
        val = str(v).strip()
        if key in TOOL_PRESET_KEYS and val:
            normalized[key] = val
    return normalized


@dataclass(slots=True)
class ContextMemoryConfig:
    base_dir: str | Path | None = None
    llm_preset: str = ""
    image_llm_preset: str = ""
    tool_presets: dict[str, str] = field(default_factory=dict)
    ask_timeout: float = 180.0
    use_mock_llm: bool = False
    enable_cleaning: bool = True
    init_config: bool = True
    evidence_versions: int = 5
    auto_manage: bool = True
    max_bucket_depth: int = 3
    max_context_window: int = 200_000
    max_memory_bytes: int = 1_000_000_000
    auto_compress_trigger_ratio: float = 0.70
    auto_split_trigger_ratio: float = 0.50
    split_plan_target_items: int = 180
    split_plan_hard_cap: int = 250
    auto_split_cooldown_sec: int = 600
    auto_split_min_drop_abs: float = 0.03
    auto_split_max_round_per_manage: int = 1
    split_ingest_parallelism: int = 16
    split_ingest_delay_min: float = 1.0
    split_ingest_delay_max: float = 3.0
    optimize_leaf_loss_threshold: float = 0.03
    gc_revision_retention_days: int = 14
    gc_gray_key_retention_days: int = 45
    gc_archived_bucket_retention_days: int = 45
    query_mode_default: str = "auto"
    global_recall_top_n: int = 120
    global_recall_top_m: int = 8
    global_recall_depth_limit: int = 8
    global_recall_time_budget_ms: int = 80
    global_recall_boost_weight: float = 0.20

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ContextMemoryConfig":
        if not isinstance(data, dict):
            return cls()
        return cls(
            base_dir=data.get("base_dir"),
            llm_preset=str(data.get("llm_preset", "CONTEXT_MEMORY")),
            image_llm_preset=str(data.get("image_llm_preset", "KIMI2.6")),
            tool_presets=_normalize_tool_presets(data.get("tool_presets")),
            ask_timeout=float(data.get("ask_timeout", 180.0)),
            use_mock_llm=bool(data.get("use_mock_llm", False)),
            enable_cleaning=bool(data.get("enable_cleaning", True)),
            init_config=bool(data.get("init_config", True)),
            evidence_versions=int(data.get("evidence_versions", 5)),
            auto_manage=bool(data.get("auto_manage", True)),
            max_bucket_depth=int(data.get("max_bucket_depth", 3)),
            max_context_window=int(data.get("max_context_window", 200_000)),
            max_memory_bytes=int(data.get("max_memory_bytes", 1_000_000_000)),
            auto_compress_trigger_ratio=float(data.get("auto_compress_trigger_ratio", 0.70)),
            auto_split_trigger_ratio=float(data.get("auto_split_trigger_ratio", 0.50)),
            split_plan_target_items=int(data.get("split_plan_target_items", 180)),
            split_plan_hard_cap=int(data.get("split_plan_hard_cap", 250)),
            auto_split_cooldown_sec=int(data.get("auto_split_cooldown_sec", 600)),
            auto_split_min_drop_abs=float(data.get("auto_split_min_drop_abs", 0.03)),
            auto_split_max_round_per_manage=int(data.get("auto_split_max_round_per_manage", 1)),
            split_ingest_parallelism=int(data.get("split_ingest_parallelism", 16)),
            split_ingest_delay_min=float(data.get("split_ingest_delay_min", 1.0)),
            split_ingest_delay_max=float(data.get("split_ingest_delay_max", 3.0)),
            optimize_leaf_loss_threshold=float(data.get("optimize_leaf_loss_threshold", 0.03)),
            gc_revision_retention_days=int(data.get("gc_revision_retention_days", 14)),
            gc_gray_key_retention_days=int(data.get("gc_gray_key_retention_days", 45)),
            gc_archived_bucket_retention_days=int(data.get("gc_archived_bucket_retention_days", 45)),
            query_mode_default=str(data.get("query_mode_default", "auto")),
            global_recall_top_n=int(data.get("global_recall_top_n", 120)),
            global_recall_top_m=int(data.get("global_recall_top_m", 8)),
            global_recall_depth_limit=int(data.get("global_recall_depth_limit", 8)),
            global_recall_time_budget_ms=int(data.get("global_recall_time_budget_ms", 80)),
            global_recall_boost_weight=float(data.get("global_recall_boost_weight", 0.20)),
        )


class BucketHandle:
    def __init__(self, engine: "ContextMemoryEngineV3", bucket_id: str) -> None:
        self._engine = engine
        self.bucket_id = bucket_id

    async def _refresh_bucket_id(self) -> str:
        resolved = await self._engine.resolve_bucket_handle_id(self.bucket_id)
        self.bucket_id = resolved
        return resolved

    async def latest_bucket_id(self) -> str:
        """Return latest canonical bucket id after following redirect chain."""
        return await self._refresh_bucket_id()

    async def set_active_bucket(self, bucket_id: str | None = None) -> dict[str, Any]:
        target = str(bucket_id or "").strip()
        if not target:
            target = await self._refresh_bucket_id()
        return await self._engine.set_active_bucket(target)

    async def switch_active_bucket(self, bucket_id: str | None = None) -> dict[str, Any]:
        return await self.set_active_bucket(bucket_id)

    def get_bucket(self, bucket_id: str) -> BucketHandle:
        return self._engine.get_bucket(bucket_id)

    def list_buckets(self) -> list[BucketInfo]:
        return self._engine.list_buckets()

    async def add_memory(
        self,
        raw_text: str,
        *,
        evidence_path: str | None = None,
        key: str | None = None,
        topic: str = "",
        force_split: bool = False,
        create_new_bucket: bool = False,
        chunk_max_chars: int = 4000,
        chunk_overlap_chars: int = 200,
    ) -> AddResult:
        bucket_id = await self._refresh_bucket_id()
        return await self._engine.add_memory(
            raw_text,
            evidence_path=evidence_path,
            key=key,
            topic=topic,
            bucket_id=bucket_id,
            force_split=force_split,
            create_new_bucket=create_new_bucket,
            chunk_max_chars=chunk_max_chars,
            chunk_overlap_chars=chunk_overlap_chars,
        )

    async def query(
        self,
        query_text: str,
        *,
        top_k: int = 5,
        use_cache: bool = True,
        mode: str = "auto",
        global_recall_top_n: int | None = None,
        global_recall_top_m: int | None = None,
        global_recall_depth_limit: int | None = None,
        global_recall_time_budget_ms: int | None = None,
    ) -> QueryResult:
        bucket_id = await self._refresh_bucket_id()
        return await self._engine.query(
            query_text,
            top_k=top_k,
            use_cache=use_cache,
            bucket_id=bucket_id,
            mode=mode,
            global_recall_top_n=global_recall_top_n,
            global_recall_top_m=global_recall_top_m,
            global_recall_depth_limit=global_recall_depth_limit,
            global_recall_time_budget_ms=global_recall_time_budget_ms,
        )

    async def force_compress(self, *, reason: str = "manual") -> CompressResult:
        bucket_id = await self._refresh_bucket_id()
        return await self._engine.force_compress(reason=reason, bucket_id=bucket_id)

    async def set_bucket(
            self,
            title: str,
            *,
            summary: str = "",
            content: str = "",
            summary_locked: bool = False
    ):
        """濡傛灉瀛樺湪锛岃繑鍥炶妗讹紱涓嶅瓨鍦ㄥ垯鍒涘缓銆傜被浼煎瓧鍏哥殑setdefault"""
        bucket_id = await self._refresh_bucket_id()
        return await self._engine.set_bucket_with_id(
            title,
            bucket_id,
            summary=summary,
            content=content,
            summary_locked=summary_locked
        )

    async def create_bucket(
        self,
        *,
        title: str,
        summary: str = "",
        content: str = "",
        summary_locked: bool = False,
    ) -> BucketInfo:
        bucket_id = await self._refresh_bucket_id()
        return await self._engine.create_bucket(
            bucket_id,
            title=title,
            summary=summary,
            content=content,
            summary_locked=summary_locked,
        )

    async def create_child_bucket(
        self,
        *,
        title: str,
        summary: str = "",
        content: str = "",
        summary_locked: bool = False,
    ) -> BucketInfo:
        return await self.create_bucket(title=title, summary=summary, content=content, summary_locked=summary_locked)

    async def refresh_bucket_summary(self, *, force: bool = False) -> dict[str, Any]:
        bucket_id = await self._refresh_bucket_id()
        return await self._engine.refresh_bucket_summary(bucket_id, force=force)

    async def delete_memory(self, key: Any, *, reason: str = "") -> DeleteResult:
        return await self._engine.delete_memory(key, reason=reason)

    async def add_memory_from_file(
        self,
        file_path: str,
        *,
        topic: str = "",
        bucket_id: str | None = None,
        image_extract_hint: str = "",
        query_hint: str | None = None,
        force_split: bool = True,
        create_new_bucket: bool = False,
        chunk_max_chars: int | None = None,
        chunk_overlap_chars: int | None = None,
        dedup_in_bucket: bool = True,
        auto_optimize_after_split: bool = True,
    ) -> AddResult:
        resolved_current = await self._refresh_bucket_id()
        bucket_id = bucket_id or resolved_current
        effective_image_hint = str(image_extract_hint or "").strip() or str(query_hint or "").strip()
        return await self._engine.add_memory_from_file(
            file_path,
            topic=topic,
            bucket_id=bucket_id,
            image_extract_hint=effective_image_hint,
            query_hint=query_hint,
            force_split=force_split,
            create_new_bucket=create_new_bucket,
            chunk_max_chars=chunk_max_chars,
            chunk_overlap_chars=chunk_overlap_chars,
            dedup_in_bucket=dedup_in_bucket,
            auto_optimize_after_split=auto_optimize_after_split,
        )

    async def add_memory_from_dir(
        self,
        dir_path: str,
        *,
        bucket_id: str | None = None,
        auto_create_sub_buckets: bool = False,
        image_extract_hint: str = "",
        query_hint: str | None = None,
        force_split: bool = True,
        create_new_bucket: bool = False,
        chunk_max_chars: int | None = None,
        chunk_overlap_chars: int | None = None,
        dedup_in_bucket: bool = True,
        collect_token_usage: bool = False,
    ) -> dict[str, Any]:
        resolved_current = await self._refresh_bucket_id()
        bucket_id = bucket_id or resolved_current
        effective_image_hint = str(image_extract_hint or "").strip() or str(query_hint or "").strip()
        return await self._engine.add_memory_from_dir(
            dir_path,
            bucket_id=bucket_id,
            auto_create_sub_buckets=auto_create_sub_buckets,
            image_extract_hint=effective_image_hint,
            # query_hint=query_hint,
            force_split=force_split,
            create_new_bucket=create_new_bucket,
            chunk_max_chars=chunk_max_chars,
            chunk_overlap_chars=chunk_overlap_chars,
            dedup_in_bucket=dedup_in_bucket,
            collect_token_usage=collect_token_usage,
        )

    async def get_memory(
            self,
            key: str,
            *,
            with_evidence: bool = False,
            revision: str | None = None,
    ) -> MemoryRecord | None:
        return await self._engine.get_memory(
            key,
            with_evidence=with_evidence,
            revision=revision
        )

    async def export_memory_to_markdown(self, memory_id: str) -> dict[str, Any]:
        return await self._engine.export_memory_to_markdown(memory_id)

    async def get_evidence_content(self, key: str, *, revision: str | None = None) -> str:
        return await self._engine.get_evidence_content(key, revision=revision)

    async def list_memories(
            self,
            *,
            include_gray: bool = True,
            include_content: bool = False,
            bucket_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_current = await self._refresh_bucket_id()
        bucket_id = bucket_id or resolved_current
        return await self._engine.list_memories(
            include_gray=include_gray,
            include_content=include_content,
            bucket_id=bucket_id,
        )

    async def get_bucket_context_usage(self, *, bucket_id: str | None = None) -> dict[str, Any]:
        resolved_current = await self._refresh_bucket_id()
        bucket_id = bucket_id or resolved_current
        return await self._engine.get_bucket_context_usage(bucket_id=bucket_id)

    async def migrate_storage_paths_to_relative(self) -> dict[str, int]:
        return await self._engine.migrate_storage_paths_to_relative()

    async def set_gray(self, key: str, *, gray: bool, reason: str = "manual") -> UpdateResult:
        return await self._engine.set_gray(key, gray=gray, reason=reason)

    async def split_bucket(
            self,
            bucket_id: str = "",
            *,
            reason: str = "manual_split",
            target_groups_min: int = 2,
            target_groups_max: int = 10,
    ) -> dict[str, Any]:
        bucket_id = bucket_id or self.bucket_id
        return await self._engine.split_bucket(bucket_id, reason=reason, target_groups_min=target_groups_min,
                                               target_groups_max=target_groups_max)

    async def cleanup_expired(self) -> CleanupResult:
        return await self._engine.cleanup_expired()

    async def stats(self) -> EngineStats:
        return await self._engine.stats()

    async def optimize(self, *, reason: str = "manual_optimize") -> OptimizeResult:
        bucket_id = await self._refresh_bucket_id()
        return await self._engine.optimize(bucket_id=bucket_id, reason=reason)

    async def move_item(self, key: str, *, target_bucket_id: str, reason: str = "manual_move") -> MoveResult:
        resolved_current = await self._refresh_bucket_id()
        target = target_bucket_id or resolved_current
        return await self._engine.move_item(key=key, target_bucket_id=target, reason=reason)

    async def gc_storage(self, *, dry_run: bool = True, reason: str = "manual_gc") -> GCResult:
        return await self._engine.gc_storage(dry_run=dry_run, reason=reason)

    async def __aiter__(self) -> AsyncIterator[MemoryRecord]:
        """Iterate direct bucket records (memories + bucket nodes), excluding gray by default."""
        payload = await self.list_memories(include_gray=False)
        for rec in payload.get("memories", []):
            yield rec
        for rec in payload.get("buckets", []):
            yield rec

    def __contains__(self, item: object) -> bool:
        """Membership over direct bucket records, excluding gray by default.

        Supported item types:
        - `str`: memory key / bucket-node key / child bucket id
        - objects with `key` and/or `bucket_id`/`child_bucket_id` attributes
          (for example `MemoryRecord`, `BucketInfo`, `BucketHandle`)
        """
        eng = self._engine
        canonical, _ = eng._resolve_bucket_redirect_chain(self.bucket_id)
        if canonical:
            self.bucket_id = canonical

        try:
            bucket_id = eng._resolve_bucket_id(self.bucket_id)
        except Exception:
            return False

        targets = self._contains_targets(item, eng)
        key_targets = targets["keys"]
        bucket_targets = targets["bucket_ids"]
        if not key_targets and not bucket_targets:
            return False

        records = eng.storage.list_bucket_records(bucket_id, include_gray=False)
        for rec in records:
            if rec.key in key_targets:
                return True

            if rec.kind != BUCKET_KIND_BUCKET or not bucket_targets:
                continue

            child_raw = str(rec.child_bucket_id or "").strip()
            if not child_raw:
                continue
            try:
                child_bucket = eng._resolve_bucket_id(child_raw)
            except Exception:
                child_bucket = child_raw
            if child_bucket in bucket_targets:
                return True
        return False

    @staticmethod
    def _contains_targets(item: object, eng: "ContextMemoryEngineV3") -> dict[str, set[str]]:
        key_targets: set[str] = set()
        bucket_targets: set[str] = set()

        def _add_key(value: object) -> None:
            text = str(value or "").strip()
            if text:
                key_targets.add(text)

        def _add_bucket(value: object) -> None:
            text = str(value or "").strip()
            if not text:
                return
            try:
                text = eng._resolve_bucket_id(text)
            except Exception:
                pass
            bucket_targets.add(text)

        if isinstance(item, str):
            _add_key(item)
            _add_bucket(item)
            return {"keys": key_targets, "bucket_ids": bucket_targets}

        if item is None:
            return {"keys": key_targets, "bucket_ids": bucket_targets}

        if hasattr(item, "key"):
            _add_key(getattr(item, "key"))
        if hasattr(item, "bucket_id"):
            _add_bucket(getattr(item, "bucket_id"))
        if hasattr(item, "child_bucket_id"):
            _add_bucket(getattr(item, "child_bucket_id"))

        return {"keys": key_targets, "bucket_ids": bucket_targets}


class ContextMemoryEngineV3:
    def __init__(
        self,
        base_dir: str | Path | None = None,
        *,
        config: ContextMemoryConfig | dict[str, Any] | None = None,
        llm_preset: str = "",
        image_llm_preset: str = "",
        tool_presets: dict[str, str] | None = None,
        ask_timeout: float = 180.0,
        use_mock_llm: bool = False,
        enable_cleaning: bool = True,
        init_config: bool = True,
        evidence_versions: int = 5,
        auto_manage: bool = True,
        max_bucket_depth: int = 3,
        max_context_window: int = 256_000,
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
        query_mode_default: str = "auto",
        global_recall_top_n: int = 120,
        global_recall_top_m: int = 8,
        global_recall_depth_limit: int = 8,
        global_recall_time_budget_ms: int = 80,
        global_recall_boost_weight: float = 0.20,
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
            use_mock_llm = cfg_obj.use_mock_llm
            enable_cleaning = cfg_obj.enable_cleaning
            init_config = cfg_obj.init_config
            evidence_versions = cfg_obj.evidence_versions
            auto_manage = cfg_obj.auto_manage
            max_bucket_depth = cfg_obj.max_bucket_depth
            max_context_window = cfg_obj.max_context_window
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
        self.image_llm_preset = (
            normalized_tool_presets.get("image_extract", "").strip() or image_llm_preset
        )
        self._evidence_versions = max(1, int(evidence_versions))

        self.base_dir: Path | None = None
        self.bucket_mapping: dict[str, str] = {}  # 瀛樺偍浜哄伐鍒涘缓鐨勬《鍚嶅拰瀹為檯id鏄犲皠
        self.storage: MemoryStorageV3 | _UnconfiguredStorageProxy = _UnconfiguredStorageProxy()
        self._llm_usage_store: LLMUsage | None = None
        self._image_name_mapping_store: AutoMapping[list[str]] | None = None
        self.alias_codec = AliasCodec(self.storage)
        self._alias_request_seq = 0
        if base_dir is not None:
            self._bind_storage(base_dir, evidence_versions=self._evidence_versions)
        prompt_dir = Path(__file__).resolve().parent / "prompts"
        self.pipeline = LLMPipelineV3(
            prompt_dir,
            llm_preset=llm_preset,
            tool_presets=normalized_tool_presets,
            ask_timeout=ask_timeout,
            use_mock_llm=use_mock_llm,
            enable_cleaning=enable_cleaning,
            init_config=init_config,
            usage_store=self._llm_usage_store,
            image_name_mapping=self._image_name_mapping_store,
        )
        self.auto_manage = auto_manage
        self.max_context_window = max(100_000, int(max_context_window))
        self.bm25_cache = BM25IndexCache(max_buckets=64)
        self.memory_manager = MemoryManager(max_bytes=max_memory_bytes)
        self.image_extractor = ImageTextExtractor(
            llm_preset=self.image_llm_preset,
            init_config=init_config,
        )
        self._global_meta_lock = asyncio.Lock()
        self._query_side_effect_lock = asyncio.Lock()
        self._bucket_lock_manager = BucketLockManager()
        # Keep legacy name for backward compatibility with existing internal code paths.
        self._lock = self._global_meta_lock
        self._query_side_effect_queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=20_000)
        self._query_side_effect_worker: asyncio.Task | None = None

        self._negative_delete_threshold = 0.10
        self._max_depth = max(1, int(max_bucket_depth))
        self._max_memory_chars = 100_000
        self._default_chunk_max_chars = 4000
        self._default_chunk_overlap_chars = 200
        self._pending_bucket_summary = "pending_summary"
        self._split_ingest_parallelism = max(1, int(split_ingest_parallelism))
        self._auto_compress_trigger_ratio = max(0.0, min(float(auto_compress_trigger_ratio), 0.70))
        self._auto_split_trigger_ratio = max(0.0, min(float(auto_split_trigger_ratio), 0.50))
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
        self._query_mode_default = str(query_mode_default or "auto").strip().lower() or "auto"
        self._global_recall_top_n = max(10, int(global_recall_top_n))
        self._global_recall_top_m = max(1, int(global_recall_top_m))
        self._global_recall_depth_limit = max(1, int(global_recall_depth_limit))
        self._global_recall_time_budget_ms = max(10, int(global_recall_time_budget_ms))
        self._global_recall_boost_weight = max(0.0, min(1.0, float(global_recall_boost_weight)))
        self._runtime = ServiceRuntime(self)
        self._bucket_handle_cls = BucketHandle
        self._bucket_topology_service = BucketTopologyService(self._runtime)
        self._query_service = QueryService(self._runtime)
        self._ingest_service = IngestService(self._runtime)
        self._split_ingest_job_service = SplitIngestJobService(self._runtime)
        self._compress_split_service = CompressSplitService(self._runtime)
        self._bucket_summary_service = BucketSummaryService(self._runtime)
        self._maintenance_service = MaintenanceService(self._runtime)
        self._optimize_service = OptimizeService(self._runtime)

    def _bind_storage(self, base_dir: str | Path, *, evidence_versions: int) -> None:
        self._evidence_versions = max(1, int(evidence_versions))
        self.base_dir = Path(base_dir)
        self.bucket_mapping.clear()
        self.storage = MemoryStorageV3(self.base_dir, evidence_versions=self._evidence_versions)
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
        if hasattr(self, "pipeline"):
            self.pipeline.usage_store = self._llm_usage_store
            self.pipeline.image_name_mapping = self._image_name_mapping_store

    def apply_config(self, config: ContextMemoryConfig | dict[str, Any]) -> None:
        cfg_obj = config if isinstance(config, ContextMemoryConfig) else ContextMemoryConfig.from_dict(config)
        normalized_tool_presets = _normalize_tool_presets(cfg_obj.tool_presets)

        new_base = cfg_obj.base_dir
        if new_base is not None:
            new_base_path = Path(new_base)
            if self.base_dir is None:
                self._bind_storage(new_base_path, evidence_versions=cfg_obj.evidence_versions)
            elif new_base_path != self.base_dir:
                raise RuntimeError(
                    f"memory base_dir cannot be changed after initialization: {self.base_dir} -> {new_base_path}"
                )
            elif int(cfg_obj.evidence_versions) != int(self._evidence_versions):
                raise RuntimeError(
                    "memory evidence_versions cannot be changed after storage initialization"
                )

        self.llm_preset = cfg_obj.llm_preset
        self.tool_presets = dict(normalized_tool_presets)
        self.image_llm_preset = normalized_tool_presets.get("image_extract", "").strip() or cfg_obj.image_llm_preset

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
        self._max_depth = max(1, int(cfg_obj.max_bucket_depth))
        self.max_context_window = max(100_000, int(cfg_obj.max_context_window))
        self.memory_manager.max_bytes = max(128 * 1024 * 1024, int(cfg_obj.max_memory_bytes))
        self._auto_compress_trigger_ratio = max(0.0, min(float(cfg_obj.auto_compress_trigger_ratio), 0.70))
        self._auto_split_trigger_ratio = max(0.0, min(float(cfg_obj.auto_split_trigger_ratio), 0.50))
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
        self._query_mode_default = str(cfg_obj.query_mode_default or "auto").strip().lower() or "auto"
        self._global_recall_top_n = max(10, int(cfg_obj.global_recall_top_n))
        self._global_recall_top_m = max(1, int(cfg_obj.global_recall_top_m))
        self._global_recall_depth_limit = max(1, int(cfg_obj.global_recall_depth_limit))
        self._global_recall_time_budget_ms = max(10, int(cfg_obj.global_recall_time_budget_ms))
        self._global_recall_boost_weight = max(0.0, min(1.0, float(cfg_obj.global_recall_boost_weight)))

    def root_bucket_id(self) -> str:
        return self.storage.get_root_bucket_id()

    def active_bucket_id(self) -> str:
        return self.storage.get_active_bucket_id()

    @property
    def bucket_id(self):
        return self.active_bucket_id()

    async def set_active_bucket(self, bucket_id: str) -> dict[str, Any]:
        target = str(bucket_id or "").strip()
        if not target:
            return {"success": False, "bucket_id": "", "message": "bucket_id is required"}
        async with self._global_meta_lock:
            resolved = self._resolve_bucket_id_soft(target) or target
            info = self.storage.get_bucket_info(resolved)
            if info is None:
                return {"success": False, "bucket_id": resolved, "message": f"bucket not found: {resolved}"}
            self.storage.set_active_bucket_id(resolved)
            return {"success": True, "bucket_id": resolved, "message": "active bucket updated"}

    async def switch_active_bucket(self, bucket_id: str) -> dict[str, Any]:
        return await self.set_active_bucket(bucket_id)

    def _sync_bucket_mapping_redirect(self, *, old_ids: set[str], new_id: str) -> None:
        if not old_ids or not new_id:
            return
        if not self.bucket_mapping:
            self._load_bucket_mapping()
        changed = False
        for title, mapped in list(self.bucket_mapping.items()):
            if mapped in old_ids and mapped != new_id:
                self.bucket_mapping[title] = new_id
                changed = True
        if changed and self.base_dir is not None:
            atomic_save_json(self.bucket_mapping, self.base_dir / "bucket_mapping.json")

    def _resolve_bucket_redirect_chain(self, bucket_id: str) -> tuple[str, list[str]]:
        current = str(bucket_id or "").strip()
        if not current:
            return "", []
        lineage: list[str] = [current]
        visited: set[str] = {current}
        while True:
            info = self.storage.get_bucket_info(current)
            if info is None:
                break
            next_id = str(info.sealed_to or "").strip() if info.sealed else ""
            if not next_id or next_id in visited:
                break
            next_info = self.storage.get_bucket_info(next_id)
            if next_info is None:
                break
            current = next_id
            lineage.append(current)
            visited.add(current)
        return current, lineage

    def _resolve_bucket_id_soft(self, bucket_id: str | None) -> str:
        raw = str(bucket_id or "").strip()
        if not raw:
            return raw
        try:
            return self._resolve_bucket_id(raw)
        except Exception:
            return raw

    @asynccontextmanager
    async def _bucket_write_lock(self, bucket_id: str | None):
        resolved = self._resolve_bucket_id_soft(bucket_id) or self.active_bucket_id()
        async with self._bucket_lock_manager.acquire_many([resolved]):
            yield resolved

    @asynccontextmanager
    async def _multi_bucket_write_lock(self, bucket_ids: list[str] | tuple[str, ...] | set[str]):
        resolved = [self._resolve_bucket_id_soft(x) for x in bucket_ids]
        async with self._bucket_lock_manager.acquire_many(resolved):
            yield resolved

    async def resolve_bucket_handle_id(self, bucket_id: str) -> str:
        return await self._bucket_topology_service.resolve_bucket_handle_id(bucket_id)

    async def latest_bucket_id(self, bucket_id: str | None = None) -> str:
        """
        Resolve bucket id to the latest canonical id.

        Useful when caller stores historical bucket ids and wants the current id
        after optimize/successor redirect.
        """
        async with self._global_meta_lock:
            return self._resolve_bucket_id(bucket_id)

    def get_bucket(self, bucket_id: str) -> BucketHandle:
        return self._bucket_topology_service.get_bucket(bucket_id)

    def list_buckets(self) -> list[BucketInfo]:
        return self._bucket_topology_service.list_buckets()

    # Alias-First internal APIs (forced mode in this branch)
    def get_or_create_alias(self, bucket_id: str, real_key: str, key_type: str) -> str:
        resolved = self._resolve_bucket_id(bucket_id)
        return self.alias_codec.get_or_create_alias(resolved, real_key, key_type)

    def resolve_alias(self, bucket_id: str, alias: str, expected_type: str | None = None) -> str:
        resolved = self._resolve_bucket_id(bucket_id)
        return self.alias_codec.resolve_alias(resolved, alias, expected_type)

    def freeze_alias_map(self, bucket_id: str) -> None:
        resolved = self._resolve_bucket_id(bucket_id)
        self.alias_codec.freeze_alias_map(resolved)

    def alias_map_version(self, bucket_id: str) -> int:
        resolved = self._resolve_bucket_id(bucket_id)
        return self.alias_codec.alias_map_version(resolved)

    def build_llm_view(
        self,
        bucket_id: str,
        real_payload: Any,
        map_version: int | None = None,
        *,
        allow_create: bool = True,
    ) -> Any:
        resolved = self._resolve_bucket_id(bucket_id)
        return self.alias_codec.build_llm_view(
            resolved,
            real_payload,
            map_version=map_version,
            allow_create=allow_create,
        )

    def resolve_llm_output(self, bucket_id: str, alias_output: Any, map_version: int | None = None) -> Any:
        resolved = self._resolve_bucket_id(bucket_id)
        return self.alias_codec.resolve_llm_output(resolved, alias_output, map_version=map_version)

    def assert_alias_only_payload(self, bucket_id: str, payload: Any) -> None:
        resolved = self._resolve_bucket_id(bucket_id)
        try:
            self.alias_codec.assert_alias_only_payload(resolved, payload)
        except AliasPayloadError as exc:
            self.storage.append_alias_audit(
                {
                    "request_id": self._next_alias_request_id("failfast"),
                    "tool": "failfast",
                    "bucket_id": resolved,
                    "message": str(exc),
                    "input_hash": stable_payload_hash(payload),
                }
            )
            raise

    def _next_alias_request_id(self, tool: str) -> str:
        self._alias_request_seq += 1
        seq = self._alias_request_seq
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"{tool}_{now}_{seq}"

    def _ensure_query_side_effect_worker(self) -> None:
        task = self._query_side_effect_worker
        if task is not None and not task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._query_side_effect_worker = loop.create_task(self._query_side_effect_worker_loop())

    def _enqueue_query_side_effect(self, op: str, payload: dict[str, Any]) -> None:
        self._ensure_query_side_effect_worker()
        try:
            self._query_side_effect_queue.put_nowait((str(op), dict(payload)))
        except asyncio.QueueFull:
            self.storage.record_query_side_effect_drop()

    def _enqueue_query_side_effects(self, ops: list[tuple[str, dict[str, Any]]]) -> None:
        for op, payload in ops:
            self._enqueue_query_side_effect(op, payload)

    async def _query_side_effect_worker_loop(self) -> None:
        while True:
            op, payload = await self._query_side_effect_queue.get()
            try:
                async with self._query_side_effect_lock:
                    try:
                        if op == "set_query_cache":
                            bid = str(payload.get("bucket_id", "")).strip() or self.active_bucket_id()
                            self.storage.set_query_cache(
                                str(payload.get("cache_key", "")),
                                dict(payload.get("result", {})),
                                bucket_id=bid,
                            )
                        elif op == "record_recall_batch":
                            keys = payload.get("keys", [])
                            if isinstance(keys, list):
                                for key in keys:
                                    token = str(key).strip()
                                    if token:
                                        self.storage.record_recall(token)
                        elif op == "record_query_degraded":
                            self.storage.record_query_degraded()
                        elif op == "record_llm_usage":
                            self._record_llm_usage_values(payload.get("usage", {}))
                        elif op == "record_llm_diag":
                            self._record_llm_diag_values(payload.get("diag", {}))
                        elif op == "record_overflow_query":
                            self.storage.record_context_overflow("query")
                        elif op == "record_alias_miss_build":
                            repeat = max(1, int(payload.get("count", 1)))
                            for _ in range(repeat):
                                self.storage.record_query_alias_miss_build()
                        elif op == "record_alias_miss_resolve":
                            repeat = max(1, int(payload.get("count", 1)))
                            for _ in range(repeat):
                                self.storage.record_query_alias_miss_resolve()
                    except Exception:
                        self.storage.record_query_side_effect_drop()
            finally:
                self._query_side_effect_queue.task_done()

    def _begin_alias_session(self) -> None:
        begin = getattr(self.storage, "begin_alias_session", None)
        if callable(begin):
            begin()

    def _end_alias_session(self, *, flush: bool = True) -> None:
        end = getattr(self.storage, "end_alias_session", None)
        if callable(end):
            end(flush=flush)

    def _audit_alias_llm_call(
        self,
        *,
        tool: str,
        bucket_id: str,
        map_version: int,
        alias_input: Any,
        alias_output: Any,
    ) -> None:
        self.storage.append_alias_audit(
            {
                "request_id": self._next_alias_request_id(tool),
                "bucket_id": bucket_id,
                "map_version": int(map_version),
                "tool": tool,
                "input_hash": stable_payload_hash(alias_input),
                "output_hash": stable_payload_hash(alias_output),
                "alias_map_hash": stable_payload_hash(self.storage.load_alias_map(bucket_id)),
            }
        )

    def _load_bucket_mapping(self):
        if not self.bucket_mapping and self.base_dir is not None:
            mapping_file = self.base_dir / "bucket_mapping.json"
            if not mapping_file.is_file():
                return

            try:
                load_content: dict = json.loads(mapping_file.read_text(encoding="utf-8"))
                exist_bucket = {k: v for k, v in load_content.items() if self.storage.get_bucket_info(v)}
                self.bucket_mapping.update(exist_bucket)

            except:
                return

    def _bucket_context(self, bucket_id: str):
        cache_key = f"ctx:{bucket_id}"
        cached = self.memory_manager.get(cache_key)
        if cached is not None:
            return cached
        ctx = self.storage.load_bucket_context(bucket_id)
        if ctx is not None:
            try:
                size = len(str(ctx.to_dict())) * 2
            except Exception:
                size = 256 * 1024
            self.memory_manager.set(cache_key, ctx, bytes_estimate=size, dirty=False)
        return ctx

    def _invalidate_bucket_context_cache(self, bucket_id: str) -> None:
        self.memory_manager.remove(f"ctx:{bucket_id}")
        keep_version = self.storage.get_bucket_version(bucket_id)
        self.bm25_cache.clear_old_versions(bucket_id=bucket_id, keep_version=keep_version)

    def _record_llm_usage(self) -> None:
        self._record_llm_usage_values(self.pipeline.last_usage)

    def _record_llm_usage_values(self, usage: dict[str, Any]) -> None:
        self.storage.record_llm_usage(
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
            cached_input_tokens=usage.get("cached_input_tokens", 0),
            calls=usage.get("calls", 0),
        )

    def _record_llm_diag(self) -> None:
        self._record_llm_diag_values(self.pipeline.last_diagnostics)

    def _record_llm_diag_values(self, diag: dict[str, Any]) -> None:
        if diag.get("parse_failed", False):
            self.storage.record_llm_parse_fail()
        if diag.get("precheck_failed", False):
            self.storage.record_llm_precheck_fail()

    def _diag_failure_stage(self, diag: dict[str, Any]) -> str:
        return str(diag.get("failure_stage", "")).strip().lower()

    def _is_context_overflow_diag(self, diag: dict[str, Any]) -> bool:
        return self._diag_failure_stage(diag) == "context_overflow"

    def _record_overflow(self, *, stage: str) -> None:
        self.storage.record_context_overflow(stage)

    def _bucket_memory_count(self, bucket_id: str) -> int:
        return len(
            [
                r
                for r in self.storage.list_bucket_records(bucket_id, include_gray=False)
                if r.kind == BUCKET_KIND_MEMORY
            ]
        )

    def _should_skip_auto_summary(self, info: BucketInfo | None) -> bool:
        if info is None:
            return True
        return bool(getattr(info, "summary_locked", False))

    def _new_split_ingest_pipeline(self) -> LLMPipelineV3:
        return LLMPipelineV3(
            self.pipeline.prompt_dir,
            llm_preset=self.pipeline.default_llm_preset,
            tool_presets=self.pipeline.tool_presets,
            ask_timeout=self.pipeline.ask_timeout,
            max_retries=self.pipeline.max_retries,
            use_mock_llm=self.pipeline.use_mock_llm,
            enable_cleaning=self.pipeline.enable_cleaning,
            usage_store=self.pipeline.usage_store,
            image_name_mapping=self.pipeline.image_name_mapping,
            # Config already initialized in main pipeline path.
            init_config=False,
        )

    async def _ingest_with_overflow_retry_detail(
        self,
        *,
        pipeline: LLMPipelineV3,
        bucket_id: str,
        ingest_kwargs: dict[str, Any],
        allow_retry: bool = True,
    ) -> tuple[dict[str, Any], bool, bool]:
        def _aliasize_ingest_call() -> tuple[dict[str, Any], dict[str, Any], int]:
            raw_payload = {
                "event": ingest_kwargs.get("event", "ADD"),
                "input_type": ingest_kwargs.get("input_type", ""),
                "skip_clean": bool(ingest_kwargs.get("skip_clean", False)),
                "preserve_literal": bool(ingest_kwargs.get("preserve_literal", False)),
                "split_total": ingest_kwargs.get("split_total"),
                "split_chunks": ingest_kwargs.get("split_chunks", []) or [],
                "split_keys": ingest_kwargs.get("split_keys", []) or [],
                "default_weight": ingest_kwargs.get("default_weight"),
                "evidence_text": ingest_kwargs.get("evidence_text", ""),
                "previous_record": ingest_kwargs.get("previous_record", {}) or {},
                "topic": ingest_kwargs.get("topic", ""),
                "key": ingest_kwargs.get("key", ""),
                "split_index": ingest_kwargs.get("split_index"),
                "raw_text": ingest_kwargs.get("raw_text", ""),
            }
            alias_payload = self.build_llm_view(bucket_id, raw_payload)
            map_ver = self.alias_map_version(bucket_id)
            self.assert_alias_only_payload(bucket_id, alias_payload)
            kwargs = dict(ingest_kwargs)
            for name in (
                "event",
                "input_type",
                "skip_clean",
                "preserve_literal",
                "split_total",
                "split_chunks",
                "split_keys",
                "default_weight",
                "evidence_text",
                "previous_record",
                "topic",
                "key",
                "split_index",
                "raw_text",
            ):
                kwargs[name] = alias_payload.get(name)
            return kwargs, alias_payload, map_ver

        alias_kwargs, alias_input, map_ver = _aliasize_ingest_call()
        result_alias = await pipeline.ingest(**alias_kwargs)
        self._audit_alias_llm_call(
            tool="ingest",
            bucket_id=bucket_id,
            map_version=map_ver,
            alias_input=alias_input,
            alias_output=result_alias,
        )
        result = self.resolve_llm_output(bucket_id, result_alias, map_version=map_ver)
        self._record_llm_usage_values(pipeline.last_usage)
        self._record_llm_diag_values(pipeline.last_diagnostics)
        overflow_seen = self._is_context_overflow_diag(pipeline.last_diagnostics)
        if not overflow_seen:
            return result, False, False

        self._record_overflow(stage="ingest")
        if not allow_retry:
            return result, True, True
        try:
            await self._force_compress_unlocked(bucket_id=bucket_id, reason="context_overflow_ingest_retry")
        except Exception:
            pass

        alias_kwargs_retry, alias_input_retry, map_ver_retry = _aliasize_ingest_call()
        retry_alias = await pipeline.ingest(**alias_kwargs_retry)
        self._audit_alias_llm_call(
            tool="ingest",
            bucket_id=bucket_id,
            map_version=map_ver_retry,
            alias_input=alias_input_retry,
            alias_output=retry_alias,
        )
        retry = self.resolve_llm_output(bucket_id, retry_alias, map_version=map_ver_retry)
        self._record_llm_usage_values(pipeline.last_usage)
        self._record_llm_diag_values(pipeline.last_diagnostics)
        overflow_still = self._is_context_overflow_diag(pipeline.last_diagnostics)
        if overflow_still:
            self._record_overflow(stage="ingest")
        return retry, True, overflow_still

    async def _ingest_with_overflow_retry(
        self,
        *,
        pipeline: LLMPipelineV3,
        bucket_id: str,
        ingest_kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        result, _, _ = await self._ingest_with_overflow_retry_detail(
            pipeline=pipeline,
            bucket_id=bucket_id,
            ingest_kwargs=ingest_kwargs,
            allow_retry=True,
        )
        return result

    @staticmethod
    def _append_relation_once(
        rel_list: list[dict[str, Any]],
        *,
        target: str,
        rel_type: str,
        score: float,
        note: str,
    ) -> None:
        for item in rel_list:
            if str(item.get("target", "")) == target and str(item.get("type", "")) == rel_type:
                return
        rel_list.append(
            {
                "target": target,
                "type": rel_type,
                "score": max(0.0, min(1.0, float(score))),
                "note": note,
            }
        )

    def _append_context_event(
        self,
        *,
        bucket_id: str,
        event_type: str,
        record: MemoryRecord,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if payload is None:
            payload = {}
        if str(event_type).upper() == "GRAY_SET":
            min_payload: dict[str, Any] = {}
            reason = payload.get("reason")
            from_revision = payload.get("from_revision")
            if reason is not None:
                min_payload["reason"] = reason
            if from_revision is not None:
                min_payload["from_revision"] = from_revision
            payload = min_payload
            event = {
                "event_type": event_type,
                "bucket_id": bucket_id,
                "key": record.key,
                "revision_id": record.revision_id,
                "kind": record.kind,
                "event": record.event,
                "gray": record.gray,
                "created_at": record.created_at,
                "payload": payload,
            }
        else:
            event = {
                "event_type": event_type,
                "bucket_id": bucket_id,
                "key": record.key,
                "revision_id": record.revision_id,
                "kind": record.kind,
                "title": record.title,
                "summary": record.summary,
                "content": record.content,
                "weight": record.weight,
                "gray": record.gray,
                "relations": record.relations,
                "evidence_ref": record.evidence_ref,
                "expires_at": record.expires_at,
                "created_at": record.created_at,
                "child_bucket_id": record.child_bucket_id,
                "payload": payload,
            }
        alias_event = self.build_llm_view(bucket_id, event)
        self.assert_alias_only_payload(bucket_id, alias_event)
        self.storage.append_bucket_event(bucket_id, alias_event)
        self.storage.append_event(
            event_type=event_type,
            bucket_id=bucket_id,
            key=record.key,
            revision_id=record.revision_id,
            payload=payload,
        )
        self._invalidate_bucket_context_cache(bucket_id)

    def _has_duplicate_memory_in_bucket(self, bucket_id: str, raw_text: str) -> bool:
        target = str(raw_text or "")
        if not target:
            return False
        for rec in self.storage.list_bucket_records(bucket_id, include_gray=False):
            if rec.kind != BUCKET_KIND_MEMORY:
                continue
            if str(rec.content or "") == target:
                return True
        return False

    def _filter_duplicate_chunks_in_bucket(self, bucket_id: str, chunks: list[str]) -> list[str]:
        if not chunks:
            return []
        existing_contents: set[str] = set()
        for rec in self.storage.list_bucket_records(bucket_id, include_gray=False):
            if rec.kind != BUCKET_KIND_MEMORY:
                continue
            existing_contents.add(str(rec.content or ""))
        out: list[str] = []
        for chunk in chunks:
            token = str(chunk or "")
            if not token:
                continue
            if token in existing_contents:
                continue
            existing_contents.add(token)
            out.append(token)
        return out

    def _build_record(
        self,
        *,
        key: str,
        event: str,
        ingested: dict[str, Any],
        bucket_id: str,
        evidence_ref: str,
        kind: str = BUCKET_KIND_MEMORY,
        child_bucket_id: str = "",
    ) -> MemoryRecord:
        content = str(ingested.get("content", ""))
        relations = normalize_relations(ingested.get("relations", {}))
        source_hash = hashlib.sha1(content.encode("utf-8")).hexdigest()
        return MemoryRecord(
            key=key,
            revision_id=self.storage.generate_revision_id(),
            kind=kind,
            bucket_id=bucket_id,
            title=str(ingested.get("title", "")).strip() or key,
            summary=str(ingested.get("summary", "")).strip()[:300],
            content=content,
            weight=max(0.0, min(1.0, float(ingested.get("weight", 0.5)))),
            event=str(ingested.get("event", event)),
            gray=bool(ingested.get("gray", False)),
            relations=relations,
            evidence_ref=evidence_ref,
            expires_at=ingested.get("expires_at"),
            source_hash=source_hash,
            child_bucket_id=child_bucket_id,
        )

    def _repair_sealed_child_links_unlocked(self) -> int:
        changed = 0
        records = self.storage.list_latest_records(include_gray=True)
        for rec in records:
            if rec.kind != BUCKET_KIND_BUCKET:
                continue
            child_id = str(rec.child_bucket_id or "").strip()
            if not child_id:
                continue
            child_info = self.storage.get_bucket_info(child_id)
            if child_info is None or not child_info.sealed:
                continue
            successor_id = str(child_info.sealed_to or "").strip()
            if not successor_id or successor_id == child_id:
                continue
            successor_info = self.storage.get_bucket_info(successor_id)
            if successor_info is None:
                continue

            relations = normalize_relations(rec.relations)
            relations["lifecycle_links"].append(
                {
                    "target": rec.revision_id,
                    "type": "revises",
                    "score": 1.0,
                    "note": "repair_sealed_child_redirect",
                }
            )
            patched = MemoryRecord(
                key=rec.key,
                revision_id=self.storage.generate_revision_id(),
                kind=rec.kind,
                bucket_id=rec.bucket_id,
                title=rec.title,
                summary=rec.summary,
                content=rec.content,
                weight=rec.weight,
                event="UPDATE",
                gray=rec.gray,
                relations=relations,
                evidence_ref=rec.evidence_ref,
                expires_at=rec.expires_at,
                source_hash=rec.source_hash,
                child_bucket_id=successor_id,
            )
            self.storage.write_memory_record(patched)
            self._append_context_event(
                bucket_id=rec.bucket_id,
                event_type="UPDATE",
                record=patched,
                payload={
                    "from_revision": rec.revision_id,
                    "reason": "repair_sealed_child_redirect",
                    "old_child_bucket_id": child_id,
                    "new_child_bucket_id": successor_id,
                },
            )
            changed += 1
        return changed

    def _maybe_repair_sealed_child_links_unlocked(self, *, force: bool = False) -> int:
        meta = self.storage.load_meta()
        try:
            version = int(meta.get("context_version", 0))
        except Exception:
            version = 0
        if not force and version == self._last_sealed_link_repair_version:
            return 0
        changed = self._repair_sealed_child_links_unlocked()
        meta_after = self.storage.load_meta()
        try:
            self._last_sealed_link_repair_version = int(meta_after.get("context_version", version))
        except Exception:
            self._last_sealed_link_repair_version = version
        return changed

    def _resolve_bucket_id(self, bucket_id: str | None) -> str:
        resolved = bucket_id or self.active_bucket_id()
        final_id, lineage = self._resolve_bucket_redirect_chain(resolved)
        if len(lineage) > 1 and final_id:
            self._sync_bucket_mapping_redirect(old_ids=set(lineage[:-1]), new_id=final_id)
        resolved = final_id or resolved
        info = self.storage.get_bucket_info(resolved)
        if info is None:
            raise ValueError(f"bucket not found: {resolved}")
        return resolved

    def _create_bucket_unlocked(
        self,
        parent_bucket_id: str,
        *,
        title: str,
        summary: str = "",
        content: str = "",
        summary_locked: bool = False,
    ) -> BucketInfo:
        parent_id = self._resolve_bucket_id(parent_bucket_id)
        parent = self.storage.get_bucket_info(parent_id)
        if parent is None:
            raise ValueError(f"bucket not found: {parent_id}")
        # Keep explicit failure for callers: root included, max depth is configurable.
        if parent.level >= self._max_depth:
            raise ValueError(f"bucket level exceeds limit: max depth is {self._max_depth} (root included)")

        node_key = self.storage.generate_key()
        summary_text = summary.strip()
        summary_status = "ready" if summary_text else "pending"
        child_summary = summary_text or self._pending_bucket_summary
        child = self.storage.create_bucket(
            parent_bucket_id=parent.bucket_id,
            level=parent.level + 1,
            title=title.strip() or "child bucket",
            summary=child_summary,
            node_key=node_key,
            summary_status=summary_status,
            summary_locked=bool(summary_locked),
        )

        node_ingested = {
            "title": child.title,
            "summary": child.summary[:140],
            "content": (content or child.summary)[:1000],
            "weight": 0.75,
            "event": "ADD",
            "gray": False,
            "relations": normalize_relations({}),
        }
        node_record = self._build_record(
            key=node_key,
            event="ADD",
            ingested=node_ingested,
            bucket_id=parent.bucket_id,
            evidence_ref="",
            kind=BUCKET_KIND_BUCKET,
            child_bucket_id=child.bucket_id,
        )
        self.storage.write_memory_record(node_record)
        self._append_context_event(
            bucket_id=parent.bucket_id,
            event_type="ADD",
            record=node_record,
            payload={"kind": "bucket", "child_bucket_id": child.bucket_id},
        )
        return child

    async def set_bucket_with_id(
            self,
            title: str,
            parent_bucket_id: str,
            *,
            summary: str = "",
            content: str = "",
            summary_locked: bool = False
    ) -> BucketHandle:
        """
        Get-or-create bucket by title under a parent bucket and return BucketHandle.

        Concurrency semantics:
        - This method guarantees setdefault-like behavior under concurrent calls.
        - Calls with the same `title` will return the same existing bucket handle,
          instead of raising duplicate-key errors.

        Exception semantics:
        - If bucket depth would exceed the max limit (root included, max depth = configured limit),
          this method propagates ValueError from internal create path.
        """
        async with self._bucket_write_lock(parent_bucket_id) as resolved_parent:
            async with self._global_meta_lock:
                if not self.bucket_mapping:
                    self._load_bucket_mapping()

                exist_bucket_id = self.bucket_mapping.get(title, "")
                if exist_bucket_id:
                    try:
                        resolved_existing = self._resolve_bucket_id(exist_bucket_id)
                    except ValueError:
                        resolved_existing = ""
                    if resolved_existing:
                        if self.bucket_mapping.get(title) != resolved_existing:
                            self.bucket_mapping[title] = resolved_existing
                            if self.base_dir is not None:
                                atomic_save_json(self.bucket_mapping, self.base_dir / "bucket_mapping.json")
                        await self._run_memory_gc()
                        return BucketHandle(self, resolved_existing)
                    self.bucket_mapping.pop(title, None)
                    if self.base_dir is not None:
                        atomic_save_json(self.bucket_mapping, self.base_dir / "bucket_mapping.json")

                # Create bucket under lock to keep setdefault semantics under concurrency.
                child = self._create_bucket_unlocked(
                    resolved_parent,
                    title=title,
                    summary=summary,
                    content=content,
                    summary_locked=summary_locked,
                )
                self.bucket_mapping[title] = child.bucket_id
                if self.base_dir is not None:
                    atomic_save_json(self.bucket_mapping, self.base_dir / "bucket_mapping.json")

                await self._run_memory_gc()
                return BucketHandle(self, child.bucket_id)

    async def set_bucket(
            self,
            title: str,
            *,
            summary: str = "",
            content: str = "",
            summary_locked: bool = False
    ) -> BucketHandle:
        """濡傛灉瀛樺湪锛岃繑鍥炶妗讹紱涓嶅瓨鍦ㄥ垯鍒涘缓銆傜被浼煎瓧鍏哥殑setdefault"""
        return await self.set_bucket_with_id(
            title,
            self.root_bucket_id(),
            summary=summary,
            content=content,
            summary_locked=summary_locked
        )

    async def create_bucket(
        self,
        parent_bucket_id: str,
        *,
        title: str,
        summary: str = "",
        content: str = "",
        summary_locked: bool = False,
    ) -> BucketInfo:
        """杩欎釜鎺ュ彛涓嶄細琚玹itle褰卞搷锛屽嵆浣縯itle鐩稿悓"""
        async with self._bucket_write_lock(parent_bucket_id) as resolved_parent:
            async with self._global_meta_lock:
                child = self._create_bucket_unlocked(
                    resolved_parent,
                    title=title,
                    summary=summary,
                    content=content,
                    summary_locked=summary_locked,
                )
                await self._run_memory_gc()
                return child

    async def create_child_bucket(
        self,
        parent_bucket_id: str,
        *,
        title: str,
        summary: str = "",
        content: str = "",
        summary_locked: bool = False,
    ) -> BucketInfo:
        return await self.create_bucket(
            parent_bucket_id,
            title=title,
            summary=summary,
            content=content,
            summary_locked=summary_locked,
        )

    async def _create_sibling_bucket(
        self,
        source_bucket_id: str,
        *,
        title: str,
        summary: str,
        content: str = "",
    ) -> BucketInfo:
        source = self.storage.get_bucket_info(source_bucket_id)
        if source is None:
            raise ValueError(f"bucket not found: {source_bucket_id}")
        if source.parent_bucket_id is None:
            raise ValueError("root bucket cannot create same-level sibling")
        parent = self.storage.get_bucket_info(source.parent_bucket_id)
        if parent is None:
            raise ValueError("source parent bucket missing")
        node_key = self.storage.generate_key()
        sibling = self.storage.create_bucket(
            parent_bucket_id=parent.bucket_id,
            level=source.level,
            title=title.strip() or "sibling bucket",
            summary=summary.strip() or "sibling summary",
            node_key=node_key,
            summary_status="ready",
            summary_locked=False,
        )
        node_ingested = {
            "title": sibling.title,
            "summary": sibling.summary[:140],
            "content": (content or sibling.summary)[:1000],
            "weight": 0.75,
            "event": "ADD",
            "gray": False,
            "relations": normalize_relations({}),
        }
        node_record = self._build_record(
            key=node_key,
            event="ADD",
            ingested=node_ingested,
            bucket_id=parent.bucket_id,
            evidence_ref="",
            kind=BUCKET_KIND_BUCKET,
            child_bucket_id=sibling.bucket_id,
        )
        self.storage.write_memory_record(node_record)
        self._append_context_event(
            bucket_id=parent.bucket_id,
            event_type="ADD",
            record=node_record,
            payload={"kind": "bucket", "child_bucket_id": sibling.bucket_id},
        )
        return sibling

    async def _create_bucket_auto(
        self,
        *,
        target_bucket_id: str,
        title: str,
        summary: str,
        content: str = "",
    ) -> BucketInfo:
        source = self.storage.get_bucket_info(target_bucket_id)
        if source is None:
            raise ValueError(f"bucket not found: {target_bucket_id}")
        if source.level < self._max_depth:
            return self._create_bucket_unlocked(
                source.bucket_id,
                title=title,
                summary=summary,
                content=content,
            )
        return await self._create_sibling_bucket(
            source.bucket_id,
            title=title,
            summary=summary,
            content=content,
        )

    @staticmethod
    def _is_auto_split_reason(reason: str) -> bool:
        r = str(reason or "").strip().lower()
        return r.startswith("auto_") or "post_compress_split" in r or "context_overflow" in r

    def _can_auto_split_now(self, *, bucket_id: str) -> bool:
        if self._auto_split_cooldown_sec <= 0:
            return True
        last_at_raw = self.storage.get_last_auto_split_at(bucket_id)
        last_at = parse_iso_or_none(last_at_raw)
        if last_at is None:
            return True
        if last_at.tzinfo is None:
            last_at = last_at.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - last_at).total_seconds() >= self._auto_split_cooldown_sec

    def _seal_bucket_unlocked(self, *, source_bucket_id: str, successor_bucket_id: str) -> None:
        old_map_hash = stable_payload_hash(self.storage.load_alias_map(source_bucket_id))
        source = self.storage.get_bucket_info(source_bucket_id)
        if source is None:
            return
        source.sealed = True
        source.sealed_to = successor_bucket_id
        source.archived = True
        self.storage.update_bucket_info(source)
        # Freeze the source map by exact id; do not resolve redirects to successor here.
        self.storage.freeze_alias_map(source_bucket_id)
        new_map_hash = stable_payload_hash(self.storage.load_alias_map(successor_bucket_id))
        self.storage.append_alias_audit(
            {
                "request_id": self._next_alias_request_id("seal_switch"),
                "tool": "seal_switch",
                "source_bucket_id": source_bucket_id,
                "successor_bucket_id": successor_bucket_id,
                "old_map_hash": old_map_hash,
                "new_map_hash": new_map_hash,
                "switched_at": utc_now_iso(),
            }
        )
        self._sync_bucket_mapping_redirect(old_ids={source_bucket_id}, new_id=successor_bucket_id)

    async def _rebuild_source_successor_unlocked(
        self,
        *,
        source_bucket_id: str,
        keep_keys: list[str],
        created_bucket_ids: list[str],
        reason: str,
    ) -> str:
        source = self.storage.get_bucket_info(source_bucket_id)
        if source is None:
            raise ValueError(f"bucket not found: {source_bucket_id}")

        # Successor lives at same level as source.
        successor = self.storage.create_bucket(
            parent_bucket_id=source.parent_bucket_id,
            level=source.level,
            title=f"{source.title}_successor",
            summary=source.summary or "successor bucket",
            node_key=self.storage.generate_key(),
            summary_status="ready" if source.summary.strip() else "pending",
            summary_locked=False,
        )

        # If source has parent, remove source from parent children and add successor.
        if source.parent_bucket_id:
            self.storage.remove_child_link(parent_bucket_id=source.parent_bucket_id, child_bucket_id=source_bucket_id)
            self.storage.add_child_link(parent_bucket_id=source.parent_bucket_id, child_bucket_id=successor.bucket_id)

        # Reparent newly created buckets to successor.
        for bid in created_bucket_ids:
            binfo = self.storage.get_bucket_info(bid)
            if binfo is None:
                continue
            self.storage.reparent_bucket(bucket_id=bid, new_parent_bucket_id=successor.bucket_id)

        # Move retained records into successor.
        dedup_keep = []
        seen_keep: set[str] = set()
        for k in keep_keys:
            ks = str(k).strip()
            if not ks or ks in seen_keep:
                continue
            seen_keep.add(ks)
            dedup_keep.append(ks)

        for key in dedup_keep:
            rec = self.storage.get_record(key)
            if rec is None or rec.gray:
                continue
            if rec.bucket_id != source_bucket_id:
                continue
            if rec.kind == BUCKET_KIND_BUCKET and rec.child_bucket_id:
                self.storage.reparent_bucket(bucket_id=rec.child_bucket_id, new_parent_bucket_id=successor.bucket_id)

            rel_old = normalize_relations(rec.relations)
            self._append_relation_once(
                rel_old["lifecycle_links"],
                target=rec.revision_id,
                rel_type="tombstones",
                score=1.0,
                note="successor_rebuild_out",
            )
            out_rec = MemoryRecord(
                key=rec.key,
                revision_id=self.storage.generate_revision_id(),
                kind=rec.kind,
                bucket_id=source_bucket_id,
                title=rec.title,
                summary=rec.summary,
                content=rec.content,
                weight=rec.weight,
                event="GRAY_SET",
                gray=True,
                relations=rel_old,
                evidence_ref=rec.evidence_ref,
                expires_at=rec.expires_at,
                source_hash=rec.source_hash,
                child_bucket_id=rec.child_bucket_id,
            )
            self.storage.write_memory_record(out_rec)
            self._append_context_event(
                bucket_id=source_bucket_id,
                event_type="GRAY_SET",
                record=out_rec,
                payload={"from_revision": rec.revision_id, "reason": "successor_rebuild_out"},
            )

            rel_new = normalize_relations(rec.relations)
            self._append_relation_once(
                rel_new["lifecycle_links"],
                target=out_rec.revision_id,
                rel_type="supersedes",
                score=1.0,
                note="successor_rebuild_in",
            )
            in_rec = MemoryRecord(
                key=rec.key,
                revision_id=self.storage.generate_revision_id(),
                kind=rec.kind,
                bucket_id=successor.bucket_id,
                title=rec.title,
                summary=rec.summary,
                content=rec.content,
                weight=rec.weight,
                event="MOVE_IN",
                gray=False,
                relations=rel_new,
                evidence_ref=rec.evidence_ref,
                expires_at=rec.expires_at,
                source_hash=rec.source_hash,
                child_bucket_id=rec.child_bucket_id,
            )
            self.storage.write_memory_record(in_rec)
            self._append_context_event(
                bucket_id=successor.bucket_id,
                event_type="MOVE_IN",
                record=in_rec,
                payload={"from_bucket": source_bucket_id, "from_revision": out_rec.revision_id, "reason": reason},
            )

        self._seal_bucket_unlocked(source_bucket_id=source_bucket_id, successor_bucket_id=successor.bucket_id)

        # ROOT has no special behavior in logic, but routing pointers should follow successor.
        root_id = self.root_bucket_id()
        active_id = self.active_bucket_id()
        if source_bucket_id == root_id or source_bucket_id == active_id:
            self.storage.set_root_and_active_bucket_id(successor.bucket_id)

        if self._is_auto_split_reason(reason):
            self.storage.mark_auto_split(source_bucket_id=source_bucket_id, successor_bucket_id=successor.bucket_id)
        return successor.bucket_id

    async def refresh_bucket_summary(self, bucket_id: str, *, force: bool = False) -> dict[str, Any]:
        async with self._bucket_write_lock(bucket_id) as resolved:
            return await self._bucket_summary_service.refresh_bucket_summary(resolved, force=force)

    async def _refresh_bucket_summary_unlocked(
        self,
        *,
        bucket_id: str,
        force: bool,
        reason: str,
    ) -> dict[str, Any]:
        info = self.storage.get_bucket_info(bucket_id)
        if info is None:
            return {"success": False, "bucket_id": bucket_id, "updated": False, "message": "bucket not found"}
        if info.sealed:
            return {"success": False, "bucket_id": bucket_id, "updated": False, "message": "sealed bucket is read-only"}
        if info.summary_locked and not force:
            return {
                "success": True,
                "bucket_id": bucket_id,
                "updated": False,
                "message": "summary locked",
                "summary_status": info.summary_status,
            }

        records = self.storage.list_bucket_records(bucket_id, include_gray=False)
        if not records:
            if info.summary != self._pending_bucket_summary or info.summary_status != "pending":
                info.summary = self._pending_bucket_summary
                info.summary_status = "pending"
                self.storage.update_bucket_info(info)
                self._append_bucket_summary_update_event_unlocked(
                    info=info,
                    summary=info.summary,
                    content=info.summary,
                    reason=f"{reason}:pending",
                )
            return {
                "success": True,
                "bucket_id": bucket_id,
                "updated": False,
                "message": "bucket has no active memories",
                "summary_status": info.summary_status,
            }

        alias_records = self.build_llm_view(
            bucket_id,
            {"records": [r.to_dict() for r in records]},
        ).get("records", [])
        map_ver = self.alias_map_version(bucket_id)
        summary_alias_payload = {"records": alias_records, "reason": reason}
        self.assert_alias_only_payload(bucket_id, summary_alias_payload)
        summary_out_alias = await self.pipeline.summarize_bucket(records=alias_records, reason=reason)
        self._audit_alias_llm_call(
            tool="bucket_summary",
            bucket_id=bucket_id,
            map_version=map_ver,
            alias_input=summary_alias_payload,
            alias_output=summary_out_alias,
        )
        summary_out = self.resolve_llm_output(bucket_id, summary_out_alias, map_version=map_ver)
        self._record_llm_usage()
        self._record_llm_diag()
        if self._is_context_overflow_diag(self.pipeline.last_diagnostics):
            self._record_overflow(stage="compress")

        new_summary = str(summary_out.get("summary", "")).strip()[:140] or info.summary
        new_content = str(summary_out.get("content", "")).strip()[:1000] or new_summary
        info.summary = new_summary
        info.summary_status = "ready"
        self.storage.update_bucket_info(info)
        self._append_bucket_summary_update_event_unlocked(
            info=info,
            summary=new_summary,
            content=new_content,
            reason=reason,
        )
        return {
            "success": True,
            "bucket_id": bucket_id,
            "updated": True,
            "message": "bucket summary refreshed",
            "summary_status": info.summary_status,
        }

    def _append_bucket_summary_update_event_unlocked(
        self,
        *,
        info: BucketInfo,
        summary: str,
        content: str,
        reason: str,
    ) -> None:
        if not info.node_key or not info.parent_bucket_id:
            return
        current = self.storage.get_record(info.node_key)
        if current is None or current.gray:
            return
        target_bucket_id = current.bucket_id
        bucket_info = self.storage.get_bucket_info(target_bucket_id)
        if bucket_info is not None and bucket_info.sealed:
            try:
                resolved_bucket = self._resolve_bucket_id(target_bucket_id)
            except Exception:
                return
            if not resolved_bucket:
                return
            target_bucket_id = resolved_bucket
        relations = normalize_relations(current.relations)
        relations["lifecycle_links"].append(
            {
                "target": current.revision_id,
                "type": "revises",
                "score": 1.0,
                "note": f"bucket_summary:{reason}",
            }
        )
        updated = MemoryRecord(
            key=current.key,
            revision_id=self.storage.generate_revision_id(),
            kind=current.kind,
            bucket_id=target_bucket_id,
            title=info.title or current.title,
            summary=summary[:300],
            content=content,
            weight=current.weight,
            event="UPDATE",
            gray=False,
            relations=relations,
            evidence_ref=current.evidence_ref,
            expires_at=current.expires_at,
            source_hash=hashlib.sha1(content.encode("utf-8")).hexdigest(),
            child_bucket_id=current.child_bucket_id,
        )
        self.storage.write_memory_record(updated)
        self._append_context_event(
            bucket_id=target_bucket_id,
            event_type="UPDATE",
            record=updated,
            payload={"from_revision": current.revision_id, "reason": f"bucket_summary:{reason}", "kind": "bucket"},
        )

    async def add_memory(
        self,
        raw_text: str,
        *,
        evidence_path: str | None = None,
        key: str | None = None,
        topic: str = "",
        bucket_id: str | None = None,
        force_split: bool = False,
        create_new_bucket: bool = False,
        chunk_max_chars: int | None = None,
        chunk_overlap_chars: int | None = None,
        dedup_in_bucket: bool = False,
    ) -> AddResult:
        self._begin_alias_session()
        try:
            async with self._bucket_write_lock(bucket_id) as bucket:
                memory_count_before = self._bucket_memory_count(bucket)
                text = str(raw_text or "")
                effective_force_split = bool(force_split)
                effective_create_new_bucket = bool(create_new_bucket) if effective_force_split else False
                max_chars = (
                    self._default_chunk_max_chars
                    if chunk_max_chars is None
                    else max(100, int(chunk_max_chars))
                )
                overlap_chars = (
                    self._default_chunk_overlap_chars
                    if chunk_overlap_chars is None
                    else max(0, int(chunk_overlap_chars))
                )
                if overlap_chars >= max_chars:
                    overlap_chars = max_chars // 4
    
                if effective_force_split or len(text) > self._max_memory_chars:
                    split_reason = "force_split" if effective_force_split else "oversize_auto_split"
                    return await self._add_memory_with_split(
                        raw_text=text,
                        topic=topic,
                        key=key,
                        evidence_path=evidence_path,
                        target_bucket_id=bucket,
                        create_new_bucket=effective_create_new_bucket or (len(text) > self._max_memory_chars),
                        chunk_max_chars=max_chars,
                        chunk_overlap_chars=overlap_chars,
                        apply_clean_gate=False,
                        split_reason=split_reason,
                        dedup_in_bucket=dedup_in_bucket,
                    )
    
                memory_key = key.strip() if isinstance(key, str) and key.strip() else self.storage.generate_key()
                evidence_ref = ""
                evidence_text = ""
                if evidence_path:
                    evidence_ref = self.storage.copy_evidence(evidence_path, key=memory_key)
                    evidence_text = self.storage.read_evidence(evidence_ref)
    
                clean_result = await self.pipeline.clean(raw_text=text, evidence_text=evidence_text)
                self._record_llm_usage()
                self._record_llm_diag()
                diag = self.pipeline.last_diagnostics
                if str(diag.get("degraded_reason", "")) == "clean_fallback":
                    self.storage.record_clean_fallback()
    
                if not bool(clean_result.get("accept", True)):
                    self.storage.record_clean_reject()
                    self.storage.record_ingest_blocked_by_clean()
                    reason = str(clean_result.get("reject_reason", "")).strip() or "clean rejected input"
                    return AddResult(success=False, key=memory_key, message=f"memory rejected: {reason}")

                if dedup_in_bucket and self._has_duplicate_memory_in_bucket(bucket, text):
                    return AddResult(success=False, key=memory_key, message="duplicate_in_bucket")
    
                clean_type = str(clean_result.get("input_type", "")).strip().lower()
                preserve_literal = bool(clean_result.get("preserve_literal", False)) or clean_type == "source_code"
                skip_clean = bool(clean_result.get("skip_clean", False)) or preserve_literal
                ingest_input = text if skip_clean else (str(clean_result.get("clean_text", "")).strip() or text)
                ingested = await self._ingest_with_overflow_retry(
                    pipeline=self.pipeline,
                    bucket_id=bucket,
                    ingest_kwargs={
                        "bucket_context": self._bucket_context(bucket),
                        "key": memory_key,
                        "event": "ADD",
                        "raw_text": ingest_input,
                        "evidence_text": evidence_text,
                        "topic": topic,
                        "input_type": clean_type,
                        "skip_clean": skip_clean,
                        "preserve_literal": preserve_literal,
                    },
                )
    
                record = self._build_record(
                    key=memory_key,
                    event="ADD",
                    ingested=ingested,
                    bucket_id=bucket,
                    evidence_ref=evidence_ref,
                    kind=BUCKET_KIND_MEMORY,
                    child_bucket_id="",
                )
                self.storage.write_memory_record(record)
                self._append_context_event(bucket_id=bucket, event_type="ADD", record=record, payload={"topic": topic})
                if memory_count_before == 0:
                    info = self.storage.get_bucket_info(bucket)
                    if not self._should_skip_auto_summary(info):
                        await self._refresh_bucket_summary_unlocked(
                            bucket_id=bucket,
                            force=False,
                            reason="auto_first_memory",
                        )
                await self._auto_manage_bucket(bucket)
                await self._run_memory_gc()
                return AddResult(
                    success=True,
                    key=record.key,
                    revision_id=record.revision_id,
                    message="memory added",
                    added_keys=[record.key],
                    split_performed=False,
                )
        finally:
            self._end_alias_session(flush=True)

    async def _add_memory_with_split(
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
    ) -> AddResult:
        info = self.storage.get_bucket_info(target_bucket_id)
        if info is None:
            return AddResult(success=False, message=f"bucket not found: {target_bucket_id}")

        source_text = str(raw_text or "")
        input_type = "plain"
        skip_clean = False
        preserve_literal = False
        if apply_clean_gate:
            clean_result = await self.pipeline.clean(raw_text=source_text, evidence_text="")
            self._record_llm_usage()
            self._record_llm_diag()
            diag = self.pipeline.last_diagnostics
            if str(diag.get("degraded_reason", "")) == "clean_fallback":
                self.storage.record_clean_fallback()
            if not bool(clean_result.get("accept", True)):
                self.storage.record_clean_reject()
                self.storage.record_ingest_blocked_by_clean()
                reason = str(clean_result.get("reject_reason", "")).strip() or "clean rejected input"
                return AddResult(success=False, key="", message=f"memory rejected: {reason}")
            input_type = str(clean_result.get("input_type", "")).strip().lower() or "plain"
            preserve_literal = bool(clean_result.get("preserve_literal", False)) or input_type == "source_code"
            skip_clean = bool(clean_result.get("skip_clean", False)) or preserve_literal
            source_text = source_text if skip_clean else (str(clean_result.get("clean_text", "")).strip() or source_text)

        target_bucket = target_bucket_id
        if create_new_bucket:
            sample_record = {
                "title": topic or "split bucket",
                "summary": source_text[:200],
                "content": source_text[:2000],
            }
            bucket_summary = await self.pipeline.summarize_bucket(records=[sample_record], reason="text_chunk_target_bucket")
            self._record_llm_usage()
            self._record_llm_diag()
            new_bucket = await self._create_bucket_auto(
                target_bucket_id=target_bucket_id,
                title=(topic or "split_bucket")[:80],
                summary=bucket_summary.get("summary", "split bucket"),
                content=bucket_summary.get("content", "")[:1000],
            )
            target_bucket = new_bucket.bucket_id

        chunk_plan = await self.pipeline.text_chunk(
            raw_text=source_text,
            topic=topic,
            chunk_max_chars=chunk_max_chars,
            chunk_overlap_chars=chunk_overlap_chars,
            reason=split_reason,
        )
        self._record_llm_usage()
        self._record_llm_diag()
        chunks = chunk_plan.get("chunks", [])
        if not isinstance(chunks, list):
            chunks = []
        chunk_texts = [str(x).strip() for x in chunks if str(x).strip()]
        if not chunk_texts:
            return AddResult(success=False, message="split produced empty chunks")

        if dedup_in_bucket:
            bucket_for_dedup = self._resolve_bucket_id_soft(target_bucket_id)
            chunk_texts = self._filter_duplicate_chunks_in_bucket(bucket_for_dedup, chunk_texts)
            if not chunk_texts:
                return AddResult(success=False, message="duplicate_in_bucket")

        chunk_total = len(chunk_texts)
        chunk_keys: list[str] = []
        for idx in range(chunk_total):
            if idx == 0 and isinstance(key, str) and key.strip():
                chunk_keys.append(key.strip())
            else:
                chunk_keys.append(self.storage.generate_key())

        seed_evidence_ref = ""
        evidence_text = ""
        if evidence_path:
            seed_evidence_ref = self.storage.copy_evidence(evidence_path, key=chunk_keys[0])
            evidence_text = self.storage.read_evidence(seed_evidence_ref)

        source_hash = hashlib.sha1(source_text.encode("utf-8")).hexdigest()
        batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid4().hex}"
        base_split_chunks = [
            {"index": idx + 1, "key": chunk_keys[idx], "content": chunk_texts[idx]}
            for idx in range(chunk_total)
        ]
        committed_indices: set[int] = set()
        committed_keys: set[str] = set()
        generation = 0
        rebuilt_once = False
        current_bucket_id = target_bucket

        def _build_split_chunks_payload() -> list[dict[str, Any]]:
            payload: list[dict[str, Any]] = []
            for idx in range(chunk_total):
                key_i = chunk_keys[idx]
                rec_i = self.storage.get_record(key_i)
                payload.append(
                    {
                        "index": idx + 1,
                        "key": key_i,
                        "content": chunk_texts[idx],
                        "stored": bool(rec_i is not None and not rec_i.gray),
                        "bucket_id": str(rec_i.bucket_id) if rec_i is not None else "",
                        "revision_id": str(rec_i.revision_id) if rec_i is not None else "",
                    }
                )
            return payload

        def _save_job(status: str, *, message: str = "") -> None:
            self.storage.save_job_journal(
                {
                    "batch_id": batch_id,
                    "target_bucket_id": target_bucket,
                    "current_bucket_id": current_bucket_id,
                    "topic": topic,
                    "split_reason": split_reason,
                    "chunk_total": chunk_total,
                    "chunk_keys": list(chunk_keys),
                    "chunk_texts": list(chunk_texts),
                    "done_indices": sorted(committed_indices),
                    "done_keys": sorted(committed_keys),
                    "generation": int(generation),
                    "rebuilt_once": bool(rebuilt_once),
                    "status": status,
                    "source_hash": source_hash,
                    "input_type": input_type,
                    "skip_clean": bool(skip_clean),
                    "preserve_literal": bool(preserve_literal),
                    "evidence_ref_seed": seed_evidence_ref,
                    "evidence_path": str(evidence_path or ""),
                    "message": message,
                    "created_at": utc_now_iso(),
                }
            )

        _save_job("running")

        results: list[dict[str, Any] | None] = [None for _ in range(chunk_total)]
        result_bucket_ids: list[str] = ["" for _ in range(chunk_total)]
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
        fatal_recoverable_error = ""
        generation_context = self._bucket_context(current_bucket_id)
        current_split_chunks = list(base_split_chunks)

        parallelism = max(1, min(self._split_ingest_parallelism, chunk_total))
        workers = [self._new_split_ingest_pipeline() for _ in range(parallelism)]
        loop = asyncio.get_running_loop()
        launch_lock = asyncio.Lock()
        next_launch_at = loop.time()
        delay_min = max(0.0, float(self._split_ingest_delay_min))
        delay_max = max(delay_min, float(self._split_ingest_delay_max))

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
                    out, overflow_seen, _ = await self._ingest_with_overflow_retry_detail(
                        pipeline=pipe,
                        bucket_id=local_bucket,
                        allow_retry=False,
                        ingest_kwargs={
                            "bucket_context": local_context,
                            "key": chunk_keys[idx],
                            "event": "ADD",
                            "raw_text": chunk_texts[idx],
                            "evidence_text": evidence_text,
                            "topic": f"{topic} [chunk {idx + 1}/{chunk_total}]".strip(),
                            "input_type": input_type,
                            "skip_clean": skip_clean,
                            "preserve_literal": preserve_literal,
                            "split_chunks": local_split_chunks,
                            "split_keys": chunk_keys,
                            "split_index": idx + 1,
                            "split_total": chunk_total,
                            "default_weight": 0.75,
                        },
                    )
                    if overflow_seen:
                        async with state_lock:
                            work_queue.put_nowait(idx)
                            if local_generation == 0 and not rebuilt_once:
                                pause_event.clear()
                                rebuild_event.set()
                            elif not fatal_recoverable_error:
                                fatal_recoverable_error = (
                                    "recoverable_split_ingest_overflow_after_rebuild; "
                                    f"batch_id={batch_id}"
                                )
                                pause_event.clear()
                        continue

                    async with state_lock:
                        results[idx] = out
                        result_bucket_ids[idx] = local_bucket
                        done_indices.add(idx)
                except Exception as exc:
                    errors.append(f"chunk {idx + 1}: {exc}")
                    async with state_lock:
                        results[idx] = {
                            "kind": BUCKET_KIND_MEMORY,
                            "title": f"{topic or 'chunk'} #{idx + 1}",
                            "summary": chunk_texts[idx][:120],
                            "content": chunk_texts[idx],
                            "weight": 0.75,
                            "event": "ADD",
                            "gray": False,
                            "relations": normalize_relations({}),
                            "expires_at": None,
                        }
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
                            fatal_recoverable_error = (
                                "recoverable_split_ingest_generation_limit_reached; "
                                f"batch_id={batch_id}"
                            )
                        break
                    old_bucket_id = current_bucket_id

                try:
                    await self._force_compress_unlocked(
                        bucket_id=old_bucket_id,
                        reason="split_ingest_overflow_pause_switch",
                    )
                except Exception:
                    pass
                try:
                    await self._auto_manage_bucket(old_bucket_id)
                except Exception:
                    pass

                new_bucket_id = self._resolve_bucket_id(old_bucket_id)
                if not new_bucket_id:
                    new_bucket_id = old_bucket_id

                async with state_lock:
                    generation = 1
                    rebuilt_once = True
                    current_bucket_id = new_bucket_id
                    generation_context = self._bucket_context(current_bucket_id)
                    current_split_chunks = _build_split_chunks_payload()
                    pending = [i for i in range(chunk_total) if i not in done_indices]
                    queue = asyncio.Queue()
                    for i in pending:
                        queue.put_nowait(i)
                    pause_event.set()
                _save_job("running", message="payload rebuilt once after overflow")
        finally:
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        first_key = ""
        first_revision = ""
        for idx, chunk in enumerate(chunk_texts):
            if results[idx] is None:
                continue
            memory_key = chunk_keys[idx]
            if memory_key in committed_keys:
                continue
            evidence_ref = ""
            if evidence_path:
                if idx == 0 and seed_evidence_ref:
                    evidence_ref = seed_evidence_ref
                else:
                    evidence_ref = self.storage.copy_evidence(evidence_path, key=memory_key)
            ingested = results[idx]
            resolved_bucket = self._resolve_bucket_id(result_bucket_ids[idx] or current_bucket_id)
            if not resolved_bucket:
                resolved_bucket = current_bucket_id
            rec = self._build_record(
                key=memory_key,
                event="ADD",
                ingested=ingested,
                bucket_id=resolved_bucket,
                evidence_ref=evidence_ref,
                kind=BUCKET_KIND_MEMORY,
            )
            rel = normalize_relations(rec.relations)
            if idx > 0:
                self._append_relation_once(
                    rel["memory_links"],
                    target=chunk_keys[idx - 1],
                    rel_type="references",
                    score=1.0,
                    note="split_prev",
                )
                self._append_relation_once(
                    rel["dependency_links"],
                    target=chunk_keys[idx - 1],
                    rel_type="depends_on",
                    score=0.9,
                    note="split_sequence_prev",
                )
            if idx + 1 < chunk_total:
                self._append_relation_once(
                    rel["memory_links"],
                    target=chunk_keys[idx + 1],
                    rel_type="references",
                    score=1.0,
                    note="split_next",
                )
                self._append_relation_once(
                    rel["memory_links"],
                    target=chunk_keys[idx + 1],
                    rel_type="extends",
                    score=0.85,
                    note="split_sequence_next",
                )
            rec = replace(rec, relations=rel)
            self.storage.write_memory_record(rec)
            self._append_context_event(
                bucket_id=resolved_bucket,
                event_type="ADD",
                record=rec,
                payload={
                    "topic": topic,
                    "split_chunk_index": idx + 1,
                    "split_chunk_total": chunk_total,
                    "split_key_prev": chunk_keys[idx - 1] if idx > 0 else "",
                    "split_key_next": chunk_keys[idx + 1] if idx + 1 < chunk_total else "",
                    "split_reason": split_reason,
                },
            )
            if not first_key:
                first_key = rec.key
                first_revision = rec.revision_id
            committed_indices.add(idx)
            committed_keys.add(memory_key)
            _save_job("running")

        pending_after = [i for i in range(chunk_total) if i not in committed_indices]
        if fatal_recoverable_error:
            _save_job("paused", message=fatal_recoverable_error)
            await self._run_memory_gc()
            return AddResult(
                success=False,
                key=first_key,
                revision_id=first_revision,
                message=(
                    f"{fatal_recoverable_error}; committed={len(committed_indices)}/{chunk_total}; "
                    f"current_bucket={current_bucket_id}"
                ),
                added_keys=[k for k in chunk_keys if k in committed_keys],
                split_performed=True,
            )

        await self._auto_manage_bucket(current_bucket_id)
        target_info = self.storage.get_bucket_info(current_bucket_id)
        if not self._should_skip_auto_summary(target_info):
            await self._refresh_bucket_summary_unlocked(
                bucket_id=current_bucket_id,
                force=False,
                reason=f"auto_split_batch:{split_reason}",
            )
        _save_job("completed", message="ok")
        await self._run_memory_gc()
        return AddResult(
            success=True,
            key=first_key,
            revision_id=first_revision,
            message=(
                f"memory split into {chunk_total} chunks, "
                f"target_bucket={current_bucket_id}, parallel={parallelism}, errors={len(errors)}, "
                f"pending={len(pending_after)}"
            ),
            added_keys=[k for k in chunk_keys if k in committed_keys],
            split_performed=True,
        )

    async def _resume_split_job_unlocked(self, job: dict[str, Any]) -> dict[str, Any]:
        batch_id = str(job.get("batch_id", "")).strip()
        chunk_keys_raw = job.get("chunk_keys", [])
        chunk_texts_raw = job.get("chunk_texts", [])
        if not batch_id or not isinstance(chunk_keys_raw, list) or not isinstance(chunk_texts_raw, list):
            return {"batch_id": batch_id, "success": False, "message": "invalid job payload"}
        chunk_keys = [str(x).strip() for x in chunk_keys_raw]
        chunk_texts = [str(x) for x in chunk_texts_raw]
        if not chunk_keys or len(chunk_keys) != len(chunk_texts):
            return {"batch_id": batch_id, "success": False, "message": "invalid chunk sequence"}

        topic = str(job.get("topic", "")).strip()
        split_reason = str(job.get("split_reason", "resume")).strip() or "resume"
        input_type = str(job.get("input_type", "plain")).strip().lower() or "plain"
        skip_clean = bool(job.get("skip_clean", False))
        preserve_literal = bool(job.get("preserve_literal", False))
        evidence_path = str(job.get("evidence_path", "")).strip()
        chunk_total = len(chunk_keys)
        done_indices: set[int] = {
            int(i) for i in job.get("done_indices", []) if isinstance(i, int) and 0 <= int(i) < chunk_total
        }
        done_keys: set[str] = {chunk_keys[i] for i in done_indices}
        generation = int(job.get("generation", 0))
        rebuilt_once = bool(job.get("rebuilt_once", False))
        current_bucket_id = self._resolve_bucket_id(str(job.get("current_bucket_id", "")).strip())
        if not current_bucket_id:
            current_bucket_id = self._resolve_bucket_id(str(job.get("target_bucket_id", "")).strip())
        if not current_bucket_id:
            return {"batch_id": batch_id, "success": False, "message": "missing target bucket"}

        def _save(status: str, message: str = "") -> None:
            payload = dict(job)
            payload["current_bucket_id"] = current_bucket_id
            payload["generation"] = generation
            payload["rebuilt_once"] = rebuilt_once
            payload["done_indices"] = sorted(done_indices)
            payload["done_keys"] = sorted(done_keys)
            payload["status"] = status
            payload["message"] = message
            payload["updated_at"] = utc_now_iso()
            self.storage.save_job_journal(payload)

        def _build_split_chunks_payload() -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for idx in range(chunk_total):
                rec = self.storage.get_record(chunk_keys[idx])
                out.append(
                    {
                        "index": idx + 1,
                        "key": chunk_keys[idx],
                        "content": chunk_texts[idx],
                        "stored": bool(rec is not None and not rec.gray),
                        "bucket_id": str(rec.bucket_id) if rec is not None else "",
                        "revision_id": str(rec.revision_id) if rec is not None else "",
                    }
                )
            return out

        seed_evidence_ref = str(job.get("evidence_ref_seed", "")).strip()
        evidence_text = ""
        if seed_evidence_ref:
            evidence_text = self.storage.read_evidence(seed_evidence_ref)

        pending_indices = [idx for idx in range(chunk_total) if idx not in done_indices]
        if not pending_indices:
            _save("completed", "already completed")
            return {"batch_id": batch_id, "success": True, "completed": chunk_total, "pending": 0}

        for idx in pending_indices:
            attempts = 0
            ingested: dict[str, Any] | None = None
            while attempts < 2:
                attempts += 1
                context_snapshot = self._bucket_context(current_bucket_id)
                split_chunks_payload = _build_split_chunks_payload()
                out, overflow_seen, _ = await self._ingest_with_overflow_retry_detail(
                    pipeline=self.pipeline,
                    bucket_id=current_bucket_id,
                    allow_retry=False,
                    ingest_kwargs={
                        "bucket_context": context_snapshot,
                        "key": chunk_keys[idx],
                        "event": "ADD",
                        "raw_text": chunk_texts[idx],
                        "evidence_text": evidence_text,
                        "topic": f"{topic} [chunk {idx + 1}/{chunk_total}]".strip(),
                        "input_type": input_type,
                        "skip_clean": skip_clean,
                        "preserve_literal": preserve_literal,
                        "split_chunks": split_chunks_payload,
                        "split_keys": chunk_keys,
                        "split_index": idx + 1,
                        "split_total": chunk_total,
                        "default_weight": 0.75,
                    },
                )
                if not overflow_seen:
                    ingested = out
                    break
                if generation == 0 and not rebuilt_once:
                    try:
                        await self._force_compress_unlocked(
                            bucket_id=current_bucket_id,
                            reason="resume_split_ingest_overflow_switch",
                        )
                    except Exception:
                        pass
                    try:
                        await self._auto_manage_bucket(current_bucket_id)
                    except Exception:
                        pass
                    current_bucket_id = self._resolve_bucket_id(current_bucket_id) or current_bucket_id
                    generation = 1
                    rebuilt_once = True
                    _save("running", "payload rebuilt once during resume")
                    continue
                _save(
                    "paused",
                    (
                        "recoverable_split_ingest_overflow_after_rebuild;"
                        f" batch_id={batch_id}; chunk_index={idx + 1}"
                    ),
                )
                return {
                    "batch_id": batch_id,
                    "success": False,
                    "completed": len(done_indices),
                    "pending": chunk_total - len(done_indices),
                    "message": "recoverable overflow after rebuild",
                }

            if ingested is None:
                _save("paused", f"resume failed without ingest result; chunk_index={idx + 1}")
                return {
                    "batch_id": batch_id,
                    "success": False,
                    "completed": len(done_indices),
                    "pending": chunk_total - len(done_indices),
                    "message": "missing ingest result",
                }

            evidence_ref = ""
            if evidence_path:
                path_obj = Path(evidence_path)
                if path_obj.exists() and path_obj.is_file():
                    if idx == 0 and seed_evidence_ref:
                        evidence_ref = seed_evidence_ref
                    else:
                        evidence_ref = self.storage.copy_evidence(path_obj, key=chunk_keys[idx])

            rec = self._build_record(
                key=chunk_keys[idx],
                event="ADD",
                ingested=ingested,
                bucket_id=self._resolve_bucket_id(current_bucket_id) or current_bucket_id,
                evidence_ref=evidence_ref,
                kind=BUCKET_KIND_MEMORY,
            )
            rel = normalize_relations(rec.relations)
            if idx > 0:
                self._append_relation_once(
                    rel["memory_links"],
                    target=chunk_keys[idx - 1],
                    rel_type="references",
                    score=1.0,
                    note="split_prev",
                )
                self._append_relation_once(
                    rel["dependency_links"],
                    target=chunk_keys[idx - 1],
                    rel_type="depends_on",
                    score=0.9,
                    note="split_sequence_prev",
                )
            if idx + 1 < chunk_total:
                self._append_relation_once(
                    rel["memory_links"],
                    target=chunk_keys[idx + 1],
                    rel_type="references",
                    score=1.0,
                    note="split_next",
                )
                self._append_relation_once(
                    rel["memory_links"],
                    target=chunk_keys[idx + 1],
                    rel_type="extends",
                    score=0.85,
                    note="split_sequence_next",
                )
            rec = replace(rec, relations=rel)
            self.storage.write_memory_record(rec)
            self._append_context_event(
                bucket_id=rec.bucket_id,
                event_type="ADD",
                record=rec,
                payload={
                    "topic": topic,
                    "split_chunk_index": idx + 1,
                    "split_chunk_total": chunk_total,
                    "split_key_prev": chunk_keys[idx - 1] if idx > 0 else "",
                    "split_key_next": chunk_keys[idx + 1] if idx + 1 < chunk_total else "",
                    "split_reason": split_reason,
                    "resume_batch_id": batch_id,
                },
            )
            done_indices.add(idx)
            done_keys.add(chunk_keys[idx])
            _save("running", f"resumed chunk {idx + 1}/{chunk_total}")

        _save("completed", "ok")
        await self._auto_manage_bucket(current_bucket_id)
        await self._run_memory_gc()
        return {"batch_id": batch_id, "success": True, "completed": len(done_indices), "pending": 0}

    async def resume_pending_jobs(self) -> dict[str, Any]:
        self._begin_alias_session()
        try:
            async with self._global_meta_lock:
                return await self._split_ingest_job_service.resume_pending_jobs()
        finally:
            self._end_alias_session(flush=True)

    async def add_memory_from_file(
        self,
        file_path: str,
        *,
        topic: str = "",
        bucket_id: str | None = None,
        image_extract_hint: str = "",
        query_hint: str | None = None,
        force_split: bool = True,
        create_new_bucket: bool = False,
        chunk_max_chars: int | None = None,
        chunk_overlap_chars: int | None = None,
        dedup_in_bucket: bool = True,
        auto_optimize_after_split: bool = True,
    ) -> AddResult:
        effective_image_hint = str(image_extract_hint or "").strip() or str(query_hint or "").strip()
        return await self._ingest_service.add_memory_from_file(
            file_path,
            topic=topic,
            bucket_id=bucket_id,
            image_extract_hint=effective_image_hint,
            # query_hint=query_hint,
            force_split=force_split,
            create_new_bucket=create_new_bucket,
            chunk_max_chars=chunk_max_chars,
            chunk_overlap_chars=chunk_overlap_chars,
            dedup_in_bucket=dedup_in_bucket,
            auto_optimize_after_split=auto_optimize_after_split,
        )

    async def add_memory_from_dir(
        self,
        dir_path: str,
        *,
        bucket_id: str | None = None,
        auto_create_sub_buckets: bool = False,
        image_extract_hint: str = "",
        # query_hint: str | None = None,
        force_split: bool = True,
        create_new_bucket: bool = False,
        chunk_max_chars: int | None = None,
        chunk_overlap_chars: int | None = None,
        dedup_in_bucket: bool = True,
        collect_token_usage: bool = False,
    ) -> dict[str, Any]:
        # effective_image_hint = str(image_extract_hint or "").strip() or str(query_hint or "").strip()
        effective_image_hint = str(image_extract_hint or "").strip()
        root_dir = Path(dir_path).expanduser()
        if not root_dir.exists() or not root_dir.is_dir():
            return {
                "success": False,
                "message": f"directory not found: {dir_path}",
                "success_count": 0,
                "fail_count": 0,
                "skip_duplicate_count": 0,
                "added_keys": [],
                "per_file_added_keys": {},
            }

        target_bucket_id = self._resolve_bucket_id(bucket_id)
        root_info = self.storage.get_bucket_info(target_bucket_id)
        if root_info is None:
            return {
                "success": False,
                "message": f"bucket not found: {target_bucket_id}",
                "success_count": 0,
                "fail_count": 0,
                "skip_duplicate_count": 0,
                "added_keys": [],
                "per_file_added_keys": {},
            }

        files = sorted([p for p in root_dir.rglob("*") if p.is_file()])
        if not files:
            return {
                "success": True,
                "message": "empty directory",
                "success_count": 0,
                "fail_count": 0,
                "skip_duplicate_count": 0,
                "bucket_id": target_bucket_id,
                "processed_files": 0,
                "added_keys": [],
                "per_file_added_keys": {},
            }

        if auto_create_sub_buckets:
            max_rel_depth = 0
            for file in files:
                rel_parent = file.parent.relative_to(root_dir)
                depth = 0 if str(rel_parent) in {".", ""} else len(rel_parent.parts)
                if depth > max_rel_depth:
                    max_rel_depth = depth
            if int(root_info.level) + int(max_rel_depth) > int(self._max_depth):
                return {
                    "success": False,
                    "message": (
                        f"max bucket depth exceeded: root_level={root_info.level}, "
                        f"required={root_info.level + max_rel_depth}, limit={self._max_depth}"
                    ),
                    "success_count": 0,
                    "fail_count": 0,
                    "skip_duplicate_count": 0,
                    "added_keys": [],
                    "per_file_added_keys": {},
                }

        llm_before = self.storage.load_meta() if collect_token_usage else {}
        usage_before = {
            "llm_calls_total": int(llm_before.get("llm_calls_total", 0)),
            "llm_input_tokens_total": int(llm_before.get("llm_input_tokens_total", 0)),
            "llm_output_tokens_total": int(llm_before.get("llm_output_tokens_total", 0)),
            "llm_cached_input_tokens_total": int(llm_before.get("llm_cached_input_tokens_total", 0)),
        }

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
            rel_parts = () if str(rel_parent) in {".", ""} else tuple(str(x) for x in rel_parent.parts)
            current_bucket = target_bucket_id
            if auto_create_sub_buckets and rel_parts:
                path_acc: list[str] = []
                for part in rel_parts:
                    path_acc.append(part)
                    path_key = tuple(path_acc)
                    cached_bucket = dir_bucket_cache.get(path_key, "")
                    if cached_bucket:
                        current_bucket = cached_bucket
                        continue
                    child = await self.set_bucket_with_id(
                        part,
                        current_bucket,
                        summary="",
                        content="",
                        summary_locked=False,
                    )
                    current_bucket = child.bucket_id
                    dir_bucket_cache[path_key] = current_bucket

            result = await self.add_memory_from_file(
                str(file_path),
                topic=file_path.name,
                bucket_id=current_bucket,
                image_extract_hint=effective_image_hint,
                # query_hint=query_hint,
                force_split=force_split,
                create_new_bucket=create_new_bucket,
                chunk_max_chars=chunk_max_chars,
                chunk_overlap_chars=chunk_overlap_chars,
                dedup_in_bucket=dedup_in_bucket,
                auto_optimize_after_split=False,
            )

            file_added = [str(k).strip() for k in result.added_keys if str(k).strip()]
            if file_added:
                per_file_added_keys[str(file_path)] = file_added
                added_keys.extend(file_added)

            if result.success:
                success_count += 1
                print(f"[add_dir] {index}/{total} OK: {file_path}")
                continue

            msg = str(result.message or "failed")
            if msg == "duplicate_in_bucket":
                skip_duplicate_count += 1
            else:
                fail_count += 1
            details.append({"file": str(file_path), "message": msg})
            print(f"[add_dir] {index}/{total} FAIL: {file_path} | {msg}")

        optimize_result: dict[str, Any] | None = None
        if (not auto_create_sub_buckets) and success_count > 0:
            opt = await self.optimize(bucket_id=target_bucket_id, reason="batch_dir_ingest")
            optimize_result = opt.to_dict()

        out: dict[str, Any] = {
            "success": True,
            "message": "batch completed",
            "bucket_id": target_bucket_id,
            "processed_files": total,
            "success_count": success_count,
            "fail_count": fail_count,
            "skip_duplicate_count": skip_duplicate_count,
            "details": details,
            "optimize_result": optimize_result,
            "added_keys": added_keys,
            "per_file_added_keys": per_file_added_keys,
        }
        if collect_token_usage:
            llm_after = self.storage.load_meta()
            out["token_usage_delta"] = {
                "llm_calls_total": int(llm_after.get("llm_calls_total", 0)) - usage_before["llm_calls_total"],
                "llm_input_tokens_total": int(llm_after.get("llm_input_tokens_total", 0))
                - usage_before["llm_input_tokens_total"],
                "llm_output_tokens_total": int(llm_after.get("llm_output_tokens_total", 0))
                - usage_before["llm_output_tokens_total"],
                "llm_cached_input_tokens_total": int(llm_after.get("llm_cached_input_tokens_total", 0))
                - usage_before["llm_cached_input_tokens_total"],
            }
        return out

    async def get_memory(
        self,
        key: str,
        *,
        with_evidence: bool = False,
        revision: str | None = None,
    ) -> MemoryRecord | None:
        rec = self.storage.get_record(key, revision)
        if rec is None:
            return None
        if with_evidence and rec.evidence_ref:
            return replace(rec, evidence_content=self.storage.read_evidence(rec.evidence_ref))
        return rec

    async def export_memory_to_markdown(self, memory_id: str) -> dict[str, Any]:
        key = str(memory_id or "").strip()
        if not key:
            return {"success": False, "memory_id": key, "path": "", "message": "memory id is required"}
        if key in {".", ".."} or "/" in key or "\\" in key:
            return {"success": False, "memory_id": key, "path": "", "message": "invalid memory id"}

        async with self._global_meta_lock:
            # Explicitly reject bucket ids; this API only exports memory shards.
            if self.storage.get_bucket_info(key) is not None:
                return {"success": False, "memory_id": key, "path": "", "message": "bucket id is not allowed"}

            rec = self.storage.get_record(key)
            if rec is None:
                return {"success": False, "memory_id": key, "path": "", "message": "memory id not found"}

            export_root = self.base_dir / "exports" / "memory_md"
            export_root.mkdir(parents=True, exist_ok=True)
            out_path = export_root / f"{key}.md"

            metadata = rec.to_dict()
            body = str(metadata.pop("content", "") or "")
            frontmatter = yaml.safe_dump(metadata, allow_unicode=True, sort_keys=False).strip()
            markdown_text = f"---\n{frontmatter}\n---\n\n{body}"
            out_path.write_text(markdown_text, encoding="utf-8")

        final_path = str(out_path.resolve())
        print(final_path)
        return {"success": True, "memory_id": key, "path": final_path, "message": "markdown exported"}

    async def get_evidence_content(self, key: str, *, revision: str | None = None) -> str:
        return self.storage.get_evidence_content_by_key(key, revision)

    async def list_memories(
        self,
        *,
        include_gray: bool = True,
        include_content: bool = False,
        bucket_id: str | None = None,
    ) -> dict[str, Any]:
        resolved = self._resolve_bucket_id(bucket_id)
        records = self.storage.list_bucket_records(resolved, include_gray=include_gray)
        memories: list[MemoryRecord] = []
        buckets: list[MemoryRecord] = []
        for rec in records:
            out_rec = rec if include_content else replace(rec, content="")
            if rec.kind == BUCKET_KIND_BUCKET:
                buckets.append(out_rec)
            elif rec.kind == BUCKET_KIND_MEMORY:
                memories.append(out_rec)
        total_memory_count = self._count_subtree_memories(resolved, include_gray=include_gray)
        usage = await self.get_bucket_context_usage(bucket_id=resolved)
        return {
            "memories": memories,
            "buckets": buckets,
            "memory_count": len(memories),
            "total_memory_count": total_memory_count,
            "bucket_count": len(buckets),
            "estimated_tokens": int(usage.get("estimated_tokens", 0)),
            "max_context_window": int(usage.get("max_context_window", self.max_context_window)),
            "usage_ratio": float(usage.get("usage_ratio", 0.0)),
            "bucket_id": resolved,
            "include_gray": include_gray,
        }

    def _count_subtree_memories(self, root_bucket_id: str, *, include_gray: bool) -> int:
        """Count memory shards in the target bucket and all descendant child buckets."""
        all_records = self.storage.list_latest_records(include_gray=include_gray)
        records_by_bucket: dict[str, list[MemoryRecord]] = {}
        for rec in all_records:
            records_by_bucket.setdefault(rec.bucket_id, []).append(rec)

        total = 0
        visited: set[str] = set()
        stack: list[str] = [root_bucket_id]
        while stack:
            current_bucket_id = stack.pop()
            if current_bucket_id in visited:
                continue
            visited.add(current_bucket_id)
            for rec in records_by_bucket.get(current_bucket_id, []):
                if rec.kind == BUCKET_KIND_MEMORY:
                    total += 1
                    continue
                if rec.kind != BUCKET_KIND_BUCKET:
                    continue
                raw_child = str(rec.child_bucket_id or "").strip()
                if not raw_child:
                    continue
                try:
                    child_bucket_id = self._resolve_bucket_id(raw_child)
                except Exception:
                    child_bucket_id = raw_child
                if child_bucket_id and child_bucket_id not in visited:
                    stack.append(child_bucket_id)
        return total

    async def get_bucket_context_usage(self, bucket_id: str | None = None) -> dict[str, Any]:
        resolved = self._resolve_bucket_id(bucket_id)
        estimated = int(self.storage.estimate_bucket_tokens(resolved, include_gray=False))
        max_window = max(1, int(self.max_context_window))
        return {
            "bucket_id": resolved,
            "estimated_tokens": estimated,
            "max_context_window": max_window,
            "usage_ratio": max(0.0, min(1.0, float(estimated) / float(max_window))),
        }

    async def update_memory(
        self,
        key: str,
        patch_text: str,
        *,
        evidence_path: str | None = None,
    ) -> UpdateResult:
        self._begin_alias_session()
        try:
            current0 = self.storage.get_record(key)
            if current0 is None:
                return UpdateResult(success=False, key=key, message="memory key not found")
            async with self._bucket_write_lock(current0.bucket_id):
                current = self.storage.get_record(key)
                if current is None:
                    return UpdateResult(success=False, key=key, message="memory key not found")
                if current.kind != BUCKET_KIND_MEMORY:
                    return UpdateResult(success=False, key=key, message="bucket node cannot be updated as memory")
    
                evidence_ref = current.evidence_ref
                evidence_text = ""
                if evidence_path:
                    evidence_ref = self.storage.copy_evidence(evidence_path, key=key)
                    evidence_text = self.storage.read_evidence(evidence_ref)
                elif evidence_ref:
                    evidence_text = self.storage.read_evidence(evidence_ref)
    
                clean_result = await self.pipeline.clean(raw_text=patch_text, evidence_text=evidence_text)
                self._record_llm_usage()
                self._record_llm_diag()
                diag = self.pipeline.last_diagnostics
                if str(diag.get("degraded_reason", "")) == "clean_fallback":
                    self.storage.record_clean_fallback()
    
                if not bool(clean_result.get("accept", True)):
                    self.storage.record_clean_reject()
                    self.storage.record_ingest_blocked_by_clean()
                    reason = str(clean_result.get("reject_reason", "")).strip() or "clean rejected input"
                    return UpdateResult(success=False, key=key, message=f"memory update rejected: {reason}")
    
                clean_type = str(clean_result.get("input_type", "")).strip().lower()
                preserve_literal = bool(clean_result.get("preserve_literal", False)) or clean_type == "source_code"
                skip_clean = bool(clean_result.get("skip_clean", False)) or preserve_literal
                ingest_input = patch_text if skip_clean else (str(clean_result.get("clean_text", "")).strip() or patch_text)
                ingested = await self._ingest_with_overflow_retry(
                    pipeline=self.pipeline,
                    bucket_id=current.bucket_id,
                    ingest_kwargs={
                        "bucket_context": self._bucket_context(current.bucket_id),
                        "key": key,
                        "event": "UPDATE",
                        "raw_text": ingest_input,
                        "evidence_text": evidence_text,
                        "topic": "",
                        "input_type": clean_type,
                        "skip_clean": skip_clean,
                        "preserve_literal": preserve_literal,
                        "previous_record": current.to_dict(),
                    },
                )
    
                relations = normalize_relations(ingested.get("relations", {}))
                relations["lifecycle_links"].append(
                    {
                        "target": current.revision_id,
                        "type": "supersedes",
                        "score": 1.0,
                        "note": "auto lifecycle relation",
                    }
                )
                ingested["relations"] = relations
    
                record = self._build_record(
                    key=key,
                    event="UPDATE",
                    ingested=ingested,
                    bucket_id=current.bucket_id,
                    evidence_ref=evidence_ref,
                    kind=current.kind,
                    child_bucket_id=current.child_bucket_id,
                )
                self.storage.write_memory_record(record)
                self._append_context_event(
                    bucket_id=current.bucket_id,
                    event_type="UPDATE",
                    record=record,
                    payload={"from_revision": current.revision_id},
                )
                await self._auto_manage_bucket(current.bucket_id)
                await self._run_memory_gc()
                return UpdateResult(success=True, key=key, revision_id=record.revision_id, message="memory updated")
        finally:
            self._end_alias_session(flush=True)

    async def set_gray(self, key: str, *, gray: bool, reason: str = "manual") -> UpdateResult:
        current0 = self.storage.get_record(key)
        if current0 is None:
            return UpdateResult(success=False, key=key, message="memory key not found")
        async with self._bucket_write_lock(current0.bucket_id):
            current = self.storage.get_record(key)
            if current is None:
                return UpdateResult(success=False, key=key, message="memory key not found")
            if current.gray == gray:
                return UpdateResult(success=True, key=key, revision_id=current.revision_id, message="gray already set")

            event = "GRAY_SET" if gray else "GRAY_CLEAR"
            relations = normalize_relations(current.relations)
            note = "manual gray set" if gray else "manual gray clear"
            relations["lifecycle_links"].append(
                {"target": current.revision_id, "type": "revises", "score": 1.0, "note": note}
            )
            record = MemoryRecord(
                key=current.key,
                revision_id=self.storage.generate_revision_id(),
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
            )
            self.storage.write_memory_record(record)
            self._append_context_event(
                bucket_id=current.bucket_id,
                event_type=event,
                record=record,
                payload={"from_revision": current.revision_id, "reason": reason},
            )
            await self._run_memory_gc()
            return UpdateResult(success=True, key=key, revision_id=record.revision_id, message="gray state updated")

    def _resolve_delete_target_key(self, target: Any) -> str:
        if isinstance(target, MemoryRecord):
            return str(target.key).strip()
        if isinstance(target, BucketInfo):
            node_key = str(target.node_key or "").strip()
            if node_key:
                return node_key
            bid = str(target.bucket_id or "").strip()
            if bid:
                info = self.storage.get_bucket_info(bid)
                if info is not None and str(info.node_key or "").strip():
                    return str(info.node_key).strip()
                return bid
            return ""
        if isinstance(target, dict):
            key_token = str(target.get("key", "")).strip()
            if key_token:
                return key_token
            node_token = str(target.get("node_key", "")).strip()
            if node_token:
                return node_token
            bucket_token = str(target.get("bucket_id", "")).strip()
            if bucket_token:
                info = self.storage.get_bucket_info(bucket_token)
                if info is not None and str(info.node_key or "").strip():
                    return str(info.node_key).strip()
                return bucket_token
            return ""
        if isinstance(target, str):
            token = target.strip()
            if not token:
                return ""
            info = self.storage.get_bucket_info(token)
            if info is not None and str(info.node_key or "").strip():
                return str(info.node_key).strip()
            return token

        key_attr = str(getattr(target, "key", "") or "").strip()
        if key_attr:
            return key_attr
        node_attr = str(getattr(target, "node_key", "") or "").strip()
        if node_attr:
            return node_attr
        bucket_attr = str(getattr(target, "bucket_id", "") or "").strip()
        if bucket_attr:
            info = self.storage.get_bucket_info(bucket_attr)
            if info is not None and str(info.node_key or "").strip():
                return str(info.node_key).strip()
            return bucket_attr
        return ""

    async def delete_memory(self, key: Any, *, reason: str = "") -> DeleteResult:
        target_key = self._resolve_delete_target_key(key)
        if not target_key:
            return DeleteResult(success=False, key="", message="invalid delete target")

        current = self.storage.get_record(target_key)
        info: BucketInfo | None = None
        if current is not None and current.kind == BUCKET_KIND_BUCKET:
            child_id = str(current.child_bucket_id or "").strip()
            if child_id:
                info = self.storage.get_bucket_info(child_id)
        if info is None:
            info = self.storage.get_bucket_info(target_key)

        if info is not None:
            if not self.bucket_mapping:
                self._load_bucket_mapping()

            remapping = {v: k for k, v in self.bucket_mapping.items()}
            if info.bucket_id in remapping:
                self.bucket_mapping.pop(remapping[info.bucket_id], None)
                atomic_save_json(self.bucket_mapping, self.base_dir / "bucket_mapping.json")

        res = await self.set_gray(target_key, gray=True, reason=reason or "delete")
        return DeleteResult(
            success=res.success,
            key=res.key,
            revision_id=res.revision_id,
            message="memory marked gray" if res.success else res.message,
        )

    async def query(
        self,
        query_text: str,
        *,
        top_k: int = 5,
        include_gray: bool = False,
        with_evidence: bool = False,
        use_cache: bool = True,
        bucket_id: str | None = None,
        max_depth: int | None = None,
        mode: str = "auto",
        global_recall_top_n: int | None = None,
        global_recall_top_m: int | None = None,
        global_recall_depth_limit: int | None = None,
        global_recall_time_budget_ms: int | None = None,
    ) -> QueryResult:
        self._ensure_query_side_effect_worker()
        return await self._query_service.run_query(
            query_text,
            top_k=top_k,
            include_gray=include_gray,
            with_evidence=with_evidence,
            use_cache=use_cache,
            bucket_id=bucket_id,
            max_depth=max_depth,
            mode=mode,
            global_recall_top_n=global_recall_top_n,
            global_recall_top_m=global_recall_top_m,
            global_recall_depth_limit=global_recall_depth_limit,
            global_recall_time_budget_ms=global_recall_time_budget_ms,
        )

    async def _query_bucket_recursive(
        self,
        *,
        bucket_id: str,
        query_text: str,
        top_k: int,
        include_gray: bool,
        use_cache: bool,
        with_evidence: bool,
        depth: int,
        depth_limit: int,
        visited: set[str],
        mode: str = "auto",
        global_recall_top_n: int | None = None,
        global_recall_top_m: int | None = None,
        global_recall_depth_limit: int | None = None,
        global_recall_time_budget_ms: int | None = None,
        global_record_boost: dict[str, float] | None = None,
        global_bucket_boost: dict[str, float] | None = None,
    ) -> QueryResult:
        return await self._query_service.query_bucket_recursive(
            bucket_id=bucket_id,
            query_text=query_text,
            top_k=top_k,
            include_gray=include_gray,
            use_cache=use_cache,
            with_evidence=with_evidence,
            depth=depth,
            depth_limit=depth_limit,
            visited=visited,
            mode=mode,
            global_recall_top_n=max(10, int(global_recall_top_n if global_recall_top_n is not None else self._global_recall_top_n)),
            global_recall_top_m=max(1, int(global_recall_top_m if global_recall_top_m is not None else self._global_recall_top_m)),
            global_recall_depth_limit=max(
                1,
                int(
                    global_recall_depth_limit
                    if global_recall_depth_limit is not None
                    else self._global_recall_depth_limit
                ),
            ),
            global_recall_time_budget_ms=max(
                10,
                int(
                    global_recall_time_budget_ms
                    if global_recall_time_budget_ms is not None
                    else self._global_recall_time_budget_ms
                ),
            ),
            global_record_boost=global_record_boost or {},
            global_bucket_boost=global_bucket_boost or {},
        )

    def _merge_llm_bm25_matches(
        self,
        *,
        records: list[MemoryRecord],
        llm_matches: Any,
        bm25_ranked: list[tuple[MemoryRecord, float]],
        bm25_norm_map: dict[str, float],
        top_k: int,
    ) -> tuple[list[QueryMatch], int]:
        return self._query_service.merge_llm_bm25_matches(
            records=records,
            llm_matches=llm_matches,
            bm25_ranked=bm25_ranked,
            bm25_norm_map=bm25_norm_map,
            top_k=top_k,
        )

    async def _resolve_bucket_matches(
        self,
        *,
        query_text: str,
        query_matches: list[QueryMatch],
        parent_top_k: int,
        include_gray: bool,
        use_cache: bool,
        with_evidence: bool,
        depth: int,
        depth_limit: int,
        visited: set[str],
        mode: str = "auto",
        global_recall_top_n: int | None = None,
        global_recall_top_m: int | None = None,
        global_recall_depth_limit: int | None = None,
        global_recall_time_budget_ms: int | None = None,
        global_record_boost: dict[str, float] | None = None,
        global_bucket_boost: dict[str, float] | None = None,
    ) -> tuple[list[QueryMatch], str]:
        return await self._query_service.resolve_bucket_matches(
            query_text=query_text,
            query_matches=query_matches,
            parent_top_k=parent_top_k,
            include_gray=include_gray,
            use_cache=use_cache,
            with_evidence=with_evidence,
            depth=depth,
            depth_limit=depth_limit,
            visited=visited,
            mode=mode,
            global_recall_top_n=max(10, int(global_recall_top_n if global_recall_top_n is not None else self._global_recall_top_n)),
            global_recall_top_m=max(1, int(global_recall_top_m if global_recall_top_m is not None else self._global_recall_top_m)),
            global_recall_depth_limit=max(
                1,
                int(
                    global_recall_depth_limit
                    if global_recall_depth_limit is not None
                    else self._global_recall_depth_limit
                ),
            ),
            global_recall_time_budget_ms=max(
                10,
                int(
                    global_recall_time_budget_ms
                    if global_recall_time_budget_ms is not None
                    else self._global_recall_time_budget_ms
                ),
            ),
            global_record_boost=global_record_boost or {},
            global_bucket_boost=global_bucket_boost or {},
        )

    async def force_compress(self, *, reason: str = "manual", bucket_id: str | None = None) -> CompressResult:
        self._begin_alias_session()
        try:
            async with self._bucket_write_lock(bucket_id) as resolved:
                return await self._compress_split_service.force_compress(reason=reason, bucket_id=resolved)
        finally:
            self._end_alias_session(flush=True)

    async def _force_compress_unlocked(self, *, bucket_id: str, reason: str) -> CompressResult:
        source = self.storage.get_bucket_info(bucket_id)
        if source is None:
            return CompressResult(success=False, message=f"bucket not found: {bucket_id}")
        if source.sealed:
            return CompressResult(success=False, message="sealed bucket is read-only")

        latest_all = self.storage.list_bucket_records(bucket_id, include_gray=True)
        latest = [r for r in latest_all if not r.gray]
        if not latest:
            return CompressResult(success=True, message="bucket is empty")

        records = [r.to_dict() for r in latest_all]
        alias_records = self.build_llm_view(bucket_id, {"records": records}).get("records", [])
        map_ver = self.alias_map_version(bucket_id)
        estimated = self.storage.estimate_bucket_tokens(bucket_id, include_gray=True)
        compress_alias_payload = {
            "reason": reason,
            "estimated_tokens": estimated,
            "max_estimated_tokens": self.max_context_window,
            "records": alias_records,
        }
        self.assert_alias_only_payload(bucket_id, compress_alias_payload)
        plan_alias = await self.pipeline.compress(
            bucket_context=self._bucket_context(bucket_id),
            records=alias_records,
            reason=reason,
            estimated_tokens=estimated,
            max_estimated_tokens=self.max_context_window,
        )
        self._audit_alias_llm_call(
            tool="compress",
            bucket_id=bucket_id,
            map_version=map_ver,
            alias_input=compress_alias_payload,
            alias_output=plan_alias,
        )
        plan = self.resolve_llm_output(bucket_id, plan_alias, map_version=map_ver)
        self._record_llm_usage()
        self._record_llm_diag()
        if self._is_context_overflow_diag(self.pipeline.last_diagnostics):
            self._record_overflow(stage="compress")

        drop_keys = [str(k) for k in plan.get("drop_keys", []) if str(k).strip()]
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

        reweighted = plan.get("reweighted", [])
        content_updates = plan.get("content_updates", [])
        reweighted_count = 0
        rewritten_count = 0
        changed = 0
        dropped = 0

        if isinstance(reweighted, list):
            for item in reweighted:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("key", "")).strip()
                rec = survivors.get(key)
                if rec is None:
                    continue
                try:
                    new_weight = float(item.get("weight", rec.weight))
                except (TypeError, ValueError):
                    continue
                new_weight = _clamp_score(new_weight)
                if abs(new_weight - float(rec.weight)) < 1e-6:
                    continue
                survivors[key] = replace(rec, weight=new_weight)
                changed += 1
                reweighted_count += 1

        allowed_rewrite_reasons = {"conflict", "outdated", "duplicate_merge"}
        if isinstance(content_updates, list):
            for item in content_updates:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("key", "")).strip()
                new_content = str(item.get("content", "")).strip()
                rewrite_reason = str(item.get("reason", "")).strip().lower()
                if not key or not new_content or rewrite_reason not in allowed_rewrite_reasons:
                    continue
                rec = survivors.get(key)
                if rec is None:
                    continue
                new_hash = hashlib.sha1(new_content.encode("utf-8")).hexdigest()
                survivors[key] = replace(rec, content=new_content, source_hash=new_hash)
                changed += 1
                rewritten_count += 1

        for key, rec in list(survivors.items()):
            if rec.evidence_ref and (not self.storage.evidence_exists(rec.evidence_ref)):
                survivors.pop(key, None)
                drop_set.add(key)

        for key in all_keys:
            if key not in survivors:
                dropped += 1

        snapshot_path = self.storage.create_snapshot(
            summary=str(plan.get("merged_summary", "")),
            bucket_id=bucket_id,
            reason=reason,
            keep_keys=sorted(survivors.keys()),
            drop_keys=sorted(set(all_keys) - set(survivors.keys())),
        )

        est_after_chars = 0
        for rec in survivors.values():
            est_after_chars += len(rec.title) + len(rec.summary) + len(rec.content)
            for rel_name, rel_items in normalize_relations(rec.relations).items():
                est_after_chars += len(rel_name)
                for item in rel_items:
                    est_after_chars += len(json.dumps(item, ensure_ascii=False))
        est_after_tokens = max(1, est_after_chars // 3)
        if (est_after_tokens / max(1, self.max_context_window)) > self._auto_split_trigger_ratio:
            split_res = await self._split_bucket_unlocked(bucket_id=bucket_id, reason="compress_over_threshold_split")
            return CompressResult(
                success=bool(split_res.get("success", False)),
                changed=changed,
                dropped=dropped,
                reweighted=reweighted_count,
                rewritten=rewritten_count,
                message="compress estimated overflow; split executed",
            )

        successor = self._create_successor_bucket_shallow_unlocked(
            source_bucket_id=bucket_id,
            title=f"{source.title}_compress",
            summary=(str(plan.get("merged_summary", "")).strip() or source.summary or "compressed successor"),
        )
        for rec in survivors.values():
            self._write_rebuilt_record_unlocked(
                source_record=rec,
                dst_bucket_id=successor.bucket_id,
                event="COMPRESS_REBUILD",
                reason=reason,
            )

        successor_info = self.storage.get_bucket_info(successor.bucket_id)
        if successor_info is not None:
            merged_summary = str(plan.get("merged_summary", "")).strip()
            if merged_summary:
                successor_info.summary = merged_summary[:140]
                successor_info.summary_status = "ready"
                self.storage.update_bucket_info(successor_info)
                self._append_bucket_summary_update_event_unlocked(
                    info=successor_info,
                    summary=successor_info.summary,
                    content=merged_summary[:1000],
                    reason=f"compress:{reason}",
                )

        self._seal_and_switch_bucket_unlocked(
            source_bucket_id=bucket_id,
            successor_bucket_id=successor.bucket_id,
            reason=reason,
        )

        for key in (set(all_keys) - set(survivors.keys())):
            self.storage.purge_evidence_for_key(key)

        self.storage.append_event(
            event_type="COMPRESS_DONE",
            bucket_id=bucket_id,
            payload={
                "reason": reason,
                "snapshot_path": snapshot_path,
                "drop_count": dropped,
                "keep_count": len(survivors),
                "changed": changed,
                "successor_bucket_id": successor.bucket_id,
                "rebuild_mode": True,
            },
        )
        await self._apply_forgetting(successor.bucket_id, from_compress=True)
        if not self._should_skip_auto_summary(successor_info):
            await self._refresh_bucket_summary_unlocked(
                bucket_id=successor.bucket_id,
                force=False,
                reason="auto_after_compress",
            )
        return CompressResult(
            success=True,
            changed=changed,
            dropped=dropped,
            reweighted=reweighted_count,
            rewritten=rewritten_count,
            message="compressed via successor rebuild",
        )

    async def _compress_remove_missing_evidence(self, bucket_id: str) -> int:
        changed = 0
        latest = self.storage.list_bucket_records(bucket_id, include_gray=False)
        for rec in latest:
            if rec.gray:
                continue
            if not rec.evidence_ref:
                continue
            if self.storage.evidence_exists(rec.evidence_ref):
                continue
            relations = normalize_relations(rec.relations)
            relations["lifecycle_links"].append(
                {"target": rec.revision_id, "type": "tombstones", "score": 1.0, "note": "missing_evidence"}
            )
            tomb = MemoryRecord(
                key=rec.key,
                revision_id=self.storage.generate_revision_id(),
                kind=rec.kind,
                bucket_id=rec.bucket_id,
                title=rec.title,
                summary=f"{rec.summary[:220]} [GRAY_SET:MISSING_EVIDENCE]",
                content=rec.content,
                weight=rec.weight,
                event="GRAY_SET",
                gray=True,
                relations=relations,
                evidence_ref=rec.evidence_ref,
                expires_at=rec.expires_at,
                source_hash=rec.source_hash,
                child_bucket_id=rec.child_bucket_id,
            )
            self.storage.write_memory_record(tomb)
            self._append_context_event(
                bucket_id=bucket_id,
                event_type="GRAY_SET",
                record=tomb,
                payload={"reason": "missing_evidence_after_compress", "from_revision": rec.revision_id},
            )
            changed += 1
        return changed

    async def split_bucket(
        self,
        bucket_id: str,
        *,
        reason: str = "manual_split",
        target_groups_min: int = 2,
        target_groups_max: int = 10,
    ) -> dict[str, Any]:
        self._begin_alias_session()
        try:
            async with self._bucket_write_lock(bucket_id) as resolved:
                return await self._compress_split_service.split_bucket(
                    resolved,
                    reason=reason,
                    target_groups_min=target_groups_min,
                    target_groups_max=target_groups_max,
                )
        finally:
            self._end_alias_session(flush=True)

    async def optimize(
        self,
        *,
        bucket_id: str | None = None,
        reason: str = "manual_optimize",
    ) -> OptimizeResult:
        self._begin_alias_session()
        try:
            async with self._bucket_write_lock(bucket_id) as resolved:
                return await self._optimize_service.optimize(bucket_id=resolved, reason=reason)
        finally:
            self._end_alias_session(flush=True)

    async def move_item(
        self,
        key: str,
        *,
        target_bucket_id: str,
        reason: str = "manual_move",
    ) -> MoveResult:
        self._begin_alias_session()
        try:
            current = self.storage.get_record(key)
            source_bucket_id = current.bucket_id if current is not None else ""
            target_resolved = self._resolve_bucket_id_soft(target_bucket_id)
            async with self._multi_bucket_write_lock([source_bucket_id, target_resolved]):
                return await self._move_item_unlocked(key=key, target_bucket_id=target_bucket_id, reason=reason)
        finally:
            self._end_alias_session(flush=True)

    async def gc_storage(self, *, dry_run: bool = True, reason: str = "manual_gc") -> GCResult:
        async with self._global_meta_lock:
            return await self._gc_storage_unlocked(dry_run=dry_run, reason=reason)

    async def _split_bucket_unlocked(
        self,
        *,
        bucket_id: str,
        reason: str,
        target_groups_min: int = 2,
        target_groups_max: int = 10,
    ) -> dict[str, Any]:
        source = self.storage.get_bucket_info(bucket_id)
        if source is None:
            return {"success": False, "message": f"bucket not found: {bucket_id}"}

        if self._is_auto_split_reason(reason):
            if not self._can_auto_split_now(bucket_id=bucket_id):
                self.storage.record_auto_split_cooldown_skip()
                return {"success": False, "created_buckets": 0, "moved_memories": 0, "message": "split skipped by cooldown"}

        records = self.storage.list_bucket_records(bucket_id, include_gray=False)
        if len(records) < 2:
            return {"success": True, "created_buckets": 0, "moved_memories": 0, "message": "not enough records to split"}

        pressure_before, _ = self._bucket_pressure(bucket_id)
        alias_records = self.build_llm_view(
            bucket_id,
            {"records": [r.to_dict() for r in records]},
        ).get("records", [])
        map_ver = self.alias_map_version(bucket_id)
        split_alias_payload = {
            "reason": reason,
            "split_plan_target_items": self._split_plan_target_items,
            "split_plan_hard_cap": self._split_plan_hard_cap,
            "target_groups_min": target_groups_min,
            "target_groups_max": target_groups_max,
            "records": alias_records,
        }
        self.assert_alias_only_payload(bucket_id, split_alias_payload)
        split_plan_alias = await self.pipeline.bucket_split(
            bucket_context=self._bucket_context(bucket_id),
            records=alias_records,
            split_plan_target_items=self._split_plan_target_items,
            split_plan_hard_cap=self._split_plan_hard_cap,
            target_groups_min=target_groups_min,
            target_groups_max=target_groups_max,
            reason=reason,
        )
        self._audit_alias_llm_call(
            tool="split_bucket",
            bucket_id=bucket_id,
            map_version=map_ver,
            alias_input=split_alias_payload,
            alias_output=split_plan_alias,
        )
        split_plan = self.resolve_llm_output(bucket_id, split_plan_alias, map_version=map_ver)
        self._record_llm_usage()
        self._record_llm_diag()

        merge_groups_raw = split_plan.get("merge_groups", [])
        keep_items_raw = split_plan.get("keep_items", [])
        if not isinstance(merge_groups_raw, list):
            merge_groups_raw = []
        if not isinstance(keep_items_raw, list):
            keep_items_raw = []

        key_to_rec = {r.key: r for r in records}
        key_set = set(key_to_rec.keys())
        merge_groups: list[dict[str, Any]] = []
        keep_keys_set: set[str] = set()

        for g in merge_groups_raw:
            if not isinstance(g, dict):
                continue
            keys_raw = g.get("keys", [])
            if not isinstance(keys_raw, list):
                continue
            keys = [str(k).strip() for k in keys_raw if str(k).strip() in key_set]
            if not keys:
                continue
            merge_groups.append(
                {
                    "title": str(g.get("title", "")).strip() or "split_group",
                    "summary": str(g.get("summary", "")).strip()[:140] or "split group",
                    "content": str(g.get("content", "")).strip()[:1000],
                    "keys": keys,
                }
            )

        for item in keep_items_raw:
            if not isinstance(item, dict):
                continue
            keys_raw = item.get("keys", [])
            if not isinstance(keys_raw, list):
                continue
            for k in keys_raw:
                ks = str(k).strip()
                if ks in key_set:
                    keep_keys_set.add(ks)

        merge_item_count = len(merge_groups) + len(keep_items_raw)
        if merge_item_count > self._split_plan_hard_cap:
            self.storage.record_split_plan_warn()
            merge_groups = []
            keep_keys_set.clear()
        elif merge_item_count > self._split_plan_target_items:
            self.storage.record_split_plan_warn()

        if not merge_groups:
            # Local fallback: only split memory-like records into clusters.
            mem_records = [r for r in records if r.kind == BUCKET_KIND_MEMORY]
            louvain_groups = louvain_split_groups(
                mem_records,
                target_groups_min=max(2, int(target_groups_min)),
                target_groups_max=max(2, int(target_groups_max)),
            )
            for idx, g in enumerate(louvain_groups):
                if not g:
                    continue
                keys = [r.key for r in g]
                alias_records = self.build_llm_view(
                    bucket_id,
                    {"records": [x.to_dict() for x in g]},
                ).get("records", [])
                map_ver = self.alias_map_version(bucket_id)
                summary_alias_payload = {"records": alias_records, "reason": "louvain_split"}
                self.assert_alias_only_payload(bucket_id, summary_alias_payload)
                summary_alias = await self.pipeline.summarize_bucket(records=alias_records, reason="louvain_split")
                self._audit_alias_llm_call(
                    tool="bucket_summary",
                    bucket_id=bucket_id,
                    map_version=map_ver,
                    alias_input=summary_alias_payload,
                    alias_output=summary_alias,
                )
                summary = self.resolve_llm_output(bucket_id, summary_alias, map_version=map_ver)
                self._record_llm_usage()
                self._record_llm_diag()
                merge_groups.append(
                    {
                        "title": f"cluster_{idx+1}",
                        "summary": summary.get("summary", f"cluster {idx+1}"),
                        "content": summary.get("content", f"cluster {idx+1} detail")[:1000],
                        "keys": keys,
                    }
                )
            if not merge_groups:
                return {"success": False, "created_buckets": 0, "moved_memories": 0, "message": "split fallback failed"}

        # Soft preference: prefer keep bucket, prefer merge memory.
        for rec in records:
            if rec.kind == BUCKET_KIND_BUCKET:
                keep_keys_set.add(rec.key)

        created = 0
        moved = 0
        target_map: dict[str, str] = {}
        created_bucket_ids: list[str] = []

        for g in merge_groups:
            if source.level < self._max_depth:
                new_bucket = self._create_bucket_unlocked(
                    source.bucket_id,
                    title=g["title"],
                    summary=g["summary"],
                    content=g["content"],
                )
            else:
                new_bucket = await self._create_sibling_bucket(
                    source.bucket_id,
                    title=g["title"],
                    summary=g["summary"],
                    content=g["content"],
                )
            created += 1
            created_bucket_ids.append(new_bucket.bucket_id)
            for k in g["keys"]:
                target_map[k] = new_bucket.bucket_id

        for key, dst_bucket in target_map.items():
            rec = self.storage.get_record(key)
            if rec is None or rec.gray:
                continue
            if rec.bucket_id != source.bucket_id:
                continue

            # Source tombstone event.
            rel_old = normalize_relations(rec.relations)
            rel_old["lifecycle_links"].append(
                {"target": rec.revision_id, "type": "tombstones", "score": 1.0, "note": "split_move_out"}
            )
            out_rec = MemoryRecord(
                key=rec.key,
                revision_id=self.storage.generate_revision_id(),
                kind=rec.kind,
                bucket_id=source.bucket_id,
                title=rec.title,
                summary=rec.summary,
                content=rec.content,
                weight=rec.weight,
                event="GRAY_SET",
                gray=True,
                relations=rel_old,
                evidence_ref=rec.evidence_ref,
                expires_at=rec.expires_at,
                source_hash=rec.source_hash,
                child_bucket_id=rec.child_bucket_id,
            )
            self.storage.write_memory_record(out_rec)
            self._append_context_event(
                bucket_id=source.bucket_id,
                event_type="GRAY_SET",
                record=out_rec,
                payload={"from_revision": rec.revision_id, "reason": "split_move_out"},
            )

            rel_new = normalize_relations(rec.relations)
            rel_new["lifecycle_links"].append(
                {"target": out_rec.revision_id, "type": "supersedes", "score": 1.0, "note": "split_move_in"}
            )
            in_rec = MemoryRecord(
                key=rec.key,
                revision_id=self.storage.generate_revision_id(),
                kind=rec.kind,
                bucket_id=dst_bucket,
                title=rec.title,
                summary=rec.summary,
                content=rec.content,
                weight=rec.weight,
                event="MOVE_IN",
                gray=False,
                relations=rel_new,
                evidence_ref=rec.evidence_ref,
                expires_at=rec.expires_at,
                source_hash=rec.source_hash,
                child_bucket_id=rec.child_bucket_id,
            )
            self.storage.write_memory_record(in_rec)
            self._append_context_event(
                bucket_id=dst_bucket,
                event_type="MOVE_IN",
                record=in_rec,
                payload={"from_bucket": source.bucket_id, "from_revision": out_rec.revision_id},
            )
            moved += 1

        # Keep keys should not be merged.
        keep_keys = [k for k in keep_keys_set if k not in target_map]
        for bid in created_bucket_ids:
            binfo = self.storage.get_bucket_info(bid)
            if binfo is not None and binfo.node_key:
                keep_keys.append(binfo.node_key)
        # Any unassigned keys default to keep.
        assigned = set(target_map.keys()) | set(keep_keys)
        for k in key_set:
            if k not in assigned:
                keep_keys.append(k)

        successor_bucket_id = await self._rebuild_source_successor_unlocked(
            source_bucket_id=source.bucket_id,
            keep_keys=keep_keys,
            created_bucket_ids=created_bucket_ids,
            reason=reason,
        )

        self.storage.append_event(
            event_type="SPLIT_DONE",
            bucket_id=source.bucket_id,
            payload={
                "reason": reason,
                "created_buckets": created,
                "moved_memories": moved,
                "successor_bucket_id": successor_bucket_id,
            },
        )
        source_info = self.storage.get_bucket_info(source.bucket_id)
        if not self._should_skip_auto_summary(source_info):
            await self._refresh_bucket_summary_unlocked(
                bucket_id=source.bucket_id,
                force=False,
                reason="auto_after_split_source",
            )
        for bid in created_bucket_ids:
            info = self.storage.get_bucket_info(bid)
            if self._should_skip_auto_summary(info):
                continue
            await self._refresh_bucket_summary_unlocked(
                bucket_id=bid,
                force=False,
                reason="auto_after_split_target",
            )
        successor_info = self.storage.get_bucket_info(successor_bucket_id)
        if successor_info is not None and not self._should_skip_auto_summary(successor_info):
            await self._refresh_bucket_summary_unlocked(
                bucket_id=successor_bucket_id,
                force=False,
                reason="auto_after_split_successor",
            )

        pressure_after, _ = self._bucket_pressure(successor_bucket_id)
        drop_abs = pressure_before - pressure_after
        if self._is_auto_split_reason(reason) and drop_abs < self._auto_split_min_drop_abs:
            self.storage.record_auto_split_no_progress()
            return {
                "success": False,
                "created_buckets": created,
                "moved_memories": moved,
                "message": f"split no progress: before={pressure_before:.4f} after={pressure_after:.4f}",
                "successor_bucket_id": successor_bucket_id,
            }
        return {
            "success": True,
            "created_buckets": created,
            "moved_memories": moved,
            "message": "split done",
            "successor_bucket_id": successor_bucket_id,
            "pressure_before": pressure_before,
            "pressure_after": pressure_after,
        }

    def _is_bucket_descendant_unlocked(self, *, ancestor_bucket_id: str, candidate_bucket_id: str) -> bool:
        ancestor = str(ancestor_bucket_id or "").strip()
        current = str(candidate_bucket_id or "").strip()
        if not ancestor or not current:
            return False
        if ancestor == current:
            return True
        seen: set[str] = set()
        while current and current not in seen:
            seen.add(current)
            info = self.storage.get_bucket_info(current)
            if info is None or not info.parent_bucket_id:
                return False
            current = str(info.parent_bucket_id).strip()
            if current == ancestor:
                return True
        return False

    def _bucket_subtree_max_level_unlocked(self, root_bucket_id: str) -> int:
        root = str(root_bucket_id or "").strip()
        info = self.storage.get_bucket_info(root)
        if info is None:
            return 0
        max_level = int(info.level)
        for item in self.storage.list_buckets():
            bid = str(item.bucket_id or "").strip()
            if not bid:
                continue
            if self._is_bucket_descendant_unlocked(ancestor_bucket_id=root, candidate_bucket_id=bid):
                max_level = max(max_level, int(item.level))
        return max_level

    def _create_successor_bucket_shallow_unlocked(
        self,
        *,
        source_bucket_id: str,
        title: str = "",
        summary: str = "",
    ) -> BucketInfo:
        source = self.storage.get_bucket_info(source_bucket_id)
        if source is None:
            raise ValueError(f"bucket not found: {source_bucket_id}")
        successor = self.storage.create_bucket(
            parent_bucket_id=source.parent_bucket_id,
            level=source.level,
            title=(title.strip() or f"{source.title}_successor"),
            summary=(summary.strip() or source.summary or "successor bucket"),
            node_key=self.storage.generate_key(),
            summary_status="ready",
            summary_locked=False,
        )
        if source.parent_bucket_id:
            self.storage.remove_child_link(parent_bucket_id=source.parent_bucket_id, child_bucket_id=source.bucket_id)
            self.storage.add_child_link(parent_bucket_id=source.parent_bucket_id, child_bucket_id=successor.bucket_id)
        return successor

    def _seal_and_switch_bucket_unlocked(
        self,
        *,
        source_bucket_id: str,
        successor_bucket_id: str,
        reason: str,
    ) -> None:
        self._seal_bucket_unlocked(source_bucket_id=source_bucket_id, successor_bucket_id=successor_bucket_id)
        root_id = self.root_bucket_id()
        active_id = self.active_bucket_id()
        if source_bucket_id == root_id or source_bucket_id == active_id:
            self.storage.set_root_and_active_bucket_id(successor_bucket_id)
        if self._is_auto_split_reason(reason):
            self.storage.mark_auto_split(source_bucket_id=source_bucket_id, successor_bucket_id=successor_bucket_id)

    def _write_rebuilt_record_unlocked(
        self,
        *,
        source_record: MemoryRecord,
        dst_bucket_id: str,
        event: str,
        reason: str,
    ) -> MemoryRecord:
        if source_record.kind == BUCKET_KIND_BUCKET and str(source_record.child_bucket_id or "").strip():
            try:
                self.storage.reparent_bucket(
                    bucket_id=str(source_record.child_bucket_id).strip(),
                    new_parent_bucket_id=dst_bucket_id,
                )
            except Exception:
                pass
        rel = normalize_relations(source_record.relations)
        self._append_relation_once(
            rel["lifecycle_links"],
            target=source_record.revision_id,
            rel_type="supersedes",
            score=1.0,
            note=event.lower(),
        )
        in_rec = MemoryRecord(
            key=source_record.key,
            revision_id=self.storage.generate_revision_id(),
            kind=source_record.kind,
            bucket_id=dst_bucket_id,
            title=source_record.title,
            summary=source_record.summary,
            content=source_record.content,
            weight=source_record.weight,
            event=event,
            gray=False,
            relations=rel,
            evidence_ref=source_record.evidence_ref,
            expires_at=source_record.expires_at,
            source_hash=source_record.source_hash,
            child_bucket_id=source_record.child_bucket_id,
        )
        self.storage.write_memory_record(in_rec)
        self._append_context_event(
            bucket_id=dst_bucket_id,
            event_type=event,
            record=in_rec,
            payload={
                "from_bucket": source_record.bucket_id,
                "from_revision": source_record.revision_id,
                "reason": reason,
            },
        )
        return in_rec

    async def _move_item_unlocked(self, *, key: str, target_bucket_id: str, reason: str) -> MoveResult:
        key = str(key or "").strip()
        if not key:
            return MoveResult(success=False, message="key is required")
        current = self.storage.get_record(key)
        if current is None:
            return MoveResult(success=False, key=key, message="key not found")
        if current.gray:
            return MoveResult(success=False, key=key, message="gray item cannot be moved")
        source_info = self.storage.get_bucket_info(current.bucket_id)
        if source_info is None or source_info.sealed:
            return MoveResult(success=False, key=key, message="source bucket is not writable")
        target_bucket = self._resolve_bucket_id(target_bucket_id)
        target_info = self.storage.get_bucket_info(target_bucket)
        if target_info is None:
            return MoveResult(success=False, key=key, message="target bucket not found")
        if target_info.sealed:
            return MoveResult(success=False, key=key, message="target bucket is sealed")

        if current.kind == BUCKET_KIND_BUCKET:
            child_raw = str(current.child_bucket_id or "").strip()
            if not child_raw:
                return MoveResult(success=False, key=key, message="invalid bucket node: missing child_bucket_id")
            child_bucket_id = self._resolve_bucket_id(child_raw)
            if child_bucket_id == self.root_bucket_id():
                return MoveResult(success=False, key=key, message="ROOT bucket cannot be moved")
            if child_bucket_id == target_bucket:
                return MoveResult(success=False, key=key, message="bucket cannot move to itself")
            if self._is_bucket_descendant_unlocked(ancestor_bucket_id=child_bucket_id, candidate_bucket_id=target_bucket):
                return MoveResult(success=False, key=key, message="bucket cannot move to its descendant")
            child_info = self.storage.get_bucket_info(child_bucket_id)
            if child_info is None:
                return MoveResult(success=False, key=key, message="child bucket not found")
            subtree_max = self._bucket_subtree_max_level_unlocked(child_bucket_id)
            depth_span = max(0, int(subtree_max) - int(child_info.level))
            new_max_level = int(target_info.level) + 1 + depth_span
            if new_max_level > self._max_depth:
                return MoveResult(success=False, key=key, message="move would exceed max depth (3)")
            self.storage.reparent_bucket(bucket_id=child_bucket_id, new_parent_bucket_id=target_bucket)
        else:
            child_bucket_id = ""

        if current.bucket_id == target_bucket:
            return MoveResult(
                success=True,
                key=key,
                from_bucket=current.bucket_id,
                to_bucket=target_bucket,
                revision_id=current.revision_id,
                moved_kind=current.kind,
                message="already in target bucket",
            )

        out_rel = normalize_relations(current.relations)
        self._append_relation_once(
            out_rel["lifecycle_links"],
            target=current.revision_id,
            rel_type="tombstones",
            score=1.0,
            note="move_out",
        )
        out_rec = MemoryRecord(
            key=current.key,
            revision_id=self.storage.generate_revision_id(),
            kind=current.kind,
            bucket_id=current.bucket_id,
            title=current.title,
            summary=current.summary,
            content=current.content,
            weight=current.weight,
            event="GRAY_SET",
            gray=True,
            relations=out_rel,
            evidence_ref=current.evidence_ref,
            expires_at=current.expires_at,
            source_hash=current.source_hash,
            child_bucket_id=child_bucket_id or current.child_bucket_id,
        )
        self.storage.write_memory_record(out_rec)
        self._append_context_event(
            bucket_id=current.bucket_id,
            event_type="GRAY_SET",
            record=out_rec,
            payload={
                "from_revision": current.revision_id,
                "reason": reason,
                "to_bucket": target_bucket,
            },
        )

        in_rel = normalize_relations(current.relations)
        self._append_relation_once(
            in_rel["lifecycle_links"],
            target=out_rec.revision_id,
            rel_type="supersedes",
            score=1.0,
            note="move_in",
        )
        in_rec = MemoryRecord(
            key=current.key,
            revision_id=self.storage.generate_revision_id(),
            kind=current.kind,
            bucket_id=target_bucket,
            title=current.title,
            summary=current.summary,
            content=current.content,
            weight=current.weight,
            event="MOVE_IN",
            gray=False,
            relations=in_rel,
            evidence_ref=current.evidence_ref,
            expires_at=current.expires_at,
            source_hash=current.source_hash,
            child_bucket_id=child_bucket_id or current.child_bucket_id,
        )
        self.storage.write_memory_record(in_rec)
        self._append_context_event(
            bucket_id=target_bucket,
            event_type="MOVE_IN",
            record=in_rec,
            payload={
                "from_bucket": current.bucket_id,
                "from_revision": out_rec.revision_id,
                "reason": reason,
            },
        )
        return MoveResult(
            success=True,
            key=key,
            from_bucket=current.bucket_id,
            to_bucket=target_bucket,
            revision_id=in_rec.revision_id,
            moved_kind=current.kind,
            message="moved",
        )

    def _create_gc_snapshot_unlocked(self, *, reason: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        snap_dir = self.storage.snapshots_dir / f"gc_snapshot_{stamp}_{uuid4().hex[:8]}"
        snap_dir.mkdir(parents=True, exist_ok=True)
        for src in (
            self.storage.state_file,
            self.storage.meta_file,
            self.storage.bucket_tree_file,
            self.storage.events_file,
            self.storage.alias_audit_file,
        ):
            if src.exists():
                shutil.copy2(src, snap_dir / src.name)
        marker = {"reason": reason, "created_at": utc_now_iso()}
        (snap_dir / "marker.json").write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(snap_dir)

    async def _gc_storage_unlocked(self, *, dry_run: bool, reason: str) -> GCResult:
        now = datetime.now(timezone.utc)
        rev_retention = timedelta(days=int(self._gc_revision_retention_days))
        gray_retention = timedelta(days=int(self._gc_gray_key_retention_days))
        bucket_retention = timedelta(days=int(self._gc_archived_bucket_retention_days))

        counts = {"revision": 0, "key": 0, "bucket": 0, "evidence": 0}
        skipped = {"protected": 0, "referenced": 0}
        errors: list[str] = []

        state = self.storage.load_state()
        keys = state.get("keys", {})
        if not isinstance(keys, dict):
            keys = {}

        active_records = [r for r in self.storage.list_latest_records(include_gray=True) if not r.gray]
        active_targets: set[str] = set()
        active_child_buckets: set[str] = set()
        for rec in active_records:
            if rec.kind == BUCKET_KIND_BUCKET and str(rec.child_bucket_id or "").strip():
                active_child_buckets.add(str(rec.child_bucket_id).strip())
            rels = normalize_relations(rec.relations)
            for rel_items in rels.values():
                for item in rel_items:
                    tgt = str(item.get("target", "")).strip()
                    if tgt:
                        active_targets.add(tgt)

        snapshot_path = ""
        if not dry_run:
            snapshot_path = self._create_gc_snapshot_unlocked(reason=reason)

        for key, node in list(keys.items()):
            if not isinstance(node, dict):
                continue
            key_dir = self.storage.memories_dir / str(key)
            latest_rev = str(node.get("latest_revision", "")).strip()
            revision_files = sorted(key_dir.glob("*.json")) if key_dir.exists() else []
            for rf in revision_files:
                rev_id = rf.stem
                if rev_id == latest_rev:
                    continue
                rec = self.storage._json_to_memory_record(rf)
                created = parse_iso_or_none(rec.created_at) if rec is not None else None
                if created is None:
                    created = datetime.fromtimestamp(rf.stat().st_mtime, tz=timezone.utc)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                if (now - created) < rev_retention:
                    continue
                counts["revision"] += 1
                if not dry_run:
                    try:
                        rf.unlink(missing_ok=True)
                    except Exception as exc:
                        errors.append(f"revision_delete_failed:{rf}:{exc}")

            if not isinstance(node.get("gray", False), bool):
                node["gray"] = bool(node.get("gray", False))
            if not bool(node.get("gray", False)):
                continue
            if str(key) in active_targets:
                skipped["referenced"] += 1
                continue
            updated = parse_iso_or_none(str(node.get("updated_at", ""))) or parse_iso_or_none(str(node.get("created_at", "")))
            if updated is None:
                continue
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            if (now - updated) < gray_retention:
                continue
            counts["key"] += 1
            if not dry_run:
                try:
                    if key_dir.exists():
                        shutil.rmtree(key_dir, ignore_errors=True)
                    self.storage.purge_evidence_for_key(str(key))
                    keys.pop(str(key), None)
                except Exception as exc:
                    errors.append(f"key_delete_failed:{key}:{exc}")

        tree = self.storage.load_bucket_tree()
        buckets_raw = tree.get("buckets", {})
        if not isinstance(buckets_raw, dict):
            buckets_raw = {}
        root_bucket_id = str(tree.get("root_bucket_id", "")).strip()
        active_bucket_id = str(tree.get("active_bucket_id", "")).strip()
        protected_buckets = {root_bucket_id, active_bucket_id}
        sealed_successors: set[str] = set()
        for raw in buckets_raw.values():
            if not isinstance(raw, dict):
                continue
            if bool(raw.get("sealed", False)):
                dst = str(raw.get("sealed_to", "")).strip()
                if dst:
                    sealed_successors.add(dst)

        for bucket_id, raw in list(buckets_raw.items()):
            if not isinstance(raw, dict):
                continue
            if bucket_id in protected_buckets or bucket_id in sealed_successors:
                skipped["protected"] += 1
                continue
            info = BucketInfo.from_dict(raw)
            if not (info.sealed and info.archived):
                continue
            if info.children:
                skipped["referenced"] += 1
                continue
            if bucket_id in active_child_buckets:
                skipped["referenced"] += 1
                continue
            updated = parse_iso_or_none(info.updated_at) or parse_iso_or_none(info.created_at)
            if updated is None:
                continue
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            if (now - updated) < bucket_retention:
                continue
            counts["bucket"] += 1
            if not dry_run:
                try:
                    bdir = self.storage.buckets_dir / bucket_id
                    if bdir.exists():
                        shutil.rmtree(bdir, ignore_errors=True)
                    buckets_raw.pop(bucket_id, None)
                    for p_raw in buckets_raw.values():
                        if isinstance(p_raw, dict):
                            children = p_raw.get("children", [])
                            if isinstance(children, list):
                                p_raw["children"] = [c for c in children if str(c) != bucket_id]
                except Exception as exc:
                    errors.append(f"bucket_delete_failed:{bucket_id}:{exc}")

        referenced_evidence: set[str] = set()
        for node in keys.values():
            if not isinstance(node, dict):
                continue
            latest_ref = str(node.get("latest_evidence_ref", "")).strip()
            if latest_ref:
                referenced_evidence.add(latest_ref)
            hist = node.get("evidence_history", [])
            if isinstance(hist, list):
                for item in hist:
                    ref = str(item).strip()
                    if ref:
                        referenced_evidence.add(ref)
        for p in self.storage.evidence_dir.rglob("*"):
            if not p.is_file():
                continue
            rel = str(p.relative_to(self.storage.evidence_dir)).replace("\\", "/")
            if rel in referenced_evidence:
                continue
            counts["evidence"] += 1
            if not dry_run:
                try:
                    p.unlink(missing_ok=True)
                except Exception as exc:
                    errors.append(f"evidence_delete_failed:{rel}:{exc}")

        if not dry_run:
            state["keys"] = keys
            self.storage.save_state(state)
            tree["buckets"] = buckets_raw
            self.storage.save_bucket_tree(tree)

        self.storage.append_event(
            event_type="GC_STORAGE",
            bucket_id=self.active_bucket_id(),
            payload={
                "dry_run": bool(dry_run),
                "reason": reason,
                "snapshot_path": snapshot_path,
                "counts": counts,
                "skipped": skipped,
                "errors": errors[:20],
            },
        )
        if dry_run:
            return GCResult(success=True, dry_run=True, message="gc dry-run done", would_delete=counts, skipped=skipped, errors=errors)
        return GCResult(success=(len(errors) == 0), dry_run=False, message="gc done", deleted=counts, skipped=skipped, errors=errors)

    async def cleanup_expired(self) -> CleanupResult:
        async with self._global_meta_lock:
            return await self._maintenance_service.cleanup_expired()

    async def _auto_manage_bucket(self, bucket_id: str) -> None:
        if not self.auto_manage:
            return
        info = self.storage.get_bucket_info(bucket_id)
        if info is None or info.sealed:
            return
        await self._apply_forgetting(bucket_id, from_compress=False)
        pressure, count = self._bucket_pressure(bucket_id)
        did_compress = False
        did_split = False
        split_round = 0

        if pressure > self._auto_compress_trigger_ratio or count > 1000:
            await self._force_compress_unlocked(bucket_id=bucket_id, reason="auto_threshold")
            did_compress = True
            pressure, count = self._bucket_pressure(bucket_id)

        if pressure > self._auto_split_trigger_ratio or count > 1000:
            if did_split:
                self.storage.record_auto_split_guard_hit()
                return
            if split_round >= self._auto_split_max_round_per_manage:
                self.storage.record_auto_split_guard_hit()
                return
            if not self._can_auto_split_now(bucket_id=bucket_id):
                self.storage.record_auto_split_cooldown_skip()
                return
            result = await self._split_bucket_unlocked(bucket_id=bucket_id, reason="auto_post_compress")
            split_round += 1
            did_split = bool(result.get("success", False))
            if not did_split:
                self.storage.record_auto_split_guard_hit()
                return

        if did_compress and did_split:
            return

    def _bucket_pressure(self, bucket_id: str) -> tuple[float, int]:
        est = self.storage.estimate_bucket_tokens(bucket_id, include_gray=True)
        count = len(self.storage.list_bucket_records(bucket_id, include_gray=False))
        return est / max(1, self.max_context_window), count

    async def _apply_forgetting(self, bucket_id: str, *, from_compress: bool) -> None:
        now = datetime.now(timezone.utc)
        for rec in self.storage.list_bucket_records(bucket_id, include_gray=False):
            if rec.kind != BUCKET_KIND_MEMORY:
                continue
            node = self.storage.get_key_node(rec.key) or {}
            if from_compress:
                last_penalty = parse_iso_or_none(str(node.get("last_compress_penalty_at", "")))
                if last_penalty is not None and last_penalty.tzinfo is None:
                    last_penalty = last_penalty.replace(tzinfo=timezone.utc)
                if last_penalty is not None and (now - last_penalty) < timedelta(days=1):
                    continue
                self.storage.set_last_compress_penalty(rec.key)

            negative = self._calc_negative_weight(rec, node=node)
            self.storage.apply_negative_penalty(rec.key, negative)
            if rec.weight + negative < self._negative_delete_threshold:
                await self.set_gray(rec.key, gray=True, reason="auto_forget")

    def _calc_negative_weight(self, rec: MemoryRecord, *, node: dict[str, Any]) -> float:
        now = datetime.now(timezone.utc)
        created = parse_iso_or_none(rec.created_at)
        if created is None:
            created = now
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)

        last_recalled = parse_iso_or_none(str(node.get("last_recalled_at", "")))
        if last_recalled is None:
            last_recalled = created
        if last_recalled.tzinfo is None:
            last_recalled = last_recalled.replace(tzinfo=timezone.utc)

        age_days = max(0.0, (now - created).total_seconds() / 86400.0)
        idle_days = max(0.0, (now - last_recalled).total_seconds() / 86400.0)
        query_hits = max(0, int(node.get("query_hits", 0)))

        # Pure negative decay: older + idle increases penalty, higher recall slightly offsets penalty.
        penalty = 0.02 * age_days + 0.03 * idle_days - 0.01 * min(query_hits, 30)
        penalty = max(0.0, min(0.9, penalty))
        return -penalty

    def _apply_negative_weight_adjust(self, key: str, score: float) -> float:
        node = self.storage.get_key_node(key) or {}
        neg = float(node.get("last_negative_weight", 0.0))
        adjusted = score + (neg * 0.35)
        return _clamp_score(adjusted)

    async def stats(self) -> EngineStats:
        return await self._maintenance_service.stats()

    async def migrate_storage_paths_to_relative(self) -> dict[str, int]:
        async with self._global_meta_lock:
            return self.storage.migrate_paths_to_relative()

    async def _run_memory_gc(self) -> None:
        evicted = self.memory_manager.cleanup()
        if not evicted:
            self.bm25_cache.prune_to_limit(approx_limit_bytes=max(64 * 1024 * 1024, self.memory_manager.max_bytes // 3))
            return
        if self.memory_manager.aggressive_mode:
            self.bm25_cache.prune_to_limit(approx_limit_bytes=max(32 * 1024 * 1024, self.memory_manager.max_bytes // 5))


class ContextMemorySystem:
    _instance: ContextMemoryEngineV3 | None = None

    @classmethod
    def get_instance(
        cls,
        *,
        config: ContextMemoryConfig | dict[str, Any] | None = None,
        base_dir: str | Path | None = None,
        llm_preset: str = "",
        image_llm_preset: str = "",
        tool_presets: dict[str, str] | None = None,
        ask_timeout: float = 90.0,
        use_mock_llm: bool = False,
        enable_cleaning: bool = True,
        init_config: bool = True,
    ) -> ContextMemoryEngineV3:
        if cls._instance is None:
            cfg_obj: ContextMemoryConfig | None = None
            if isinstance(config, ContextMemoryConfig):
                cfg_obj = config
            elif isinstance(config, dict):
                cfg_obj = ContextMemoryConfig.from_dict(config)
            else:
                cfg_obj = ContextMemoryConfig(
                    base_dir=base_dir,
                    llm_preset=llm_preset,
                    image_llm_preset=image_llm_preset,
                    tool_presets=_normalize_tool_presets(tool_presets),
                    ask_timeout=ask_timeout,
                    use_mock_llm=use_mock_llm,
                    enable_cleaning=enable_cleaning,
                    init_config=init_config,
                )
            if cfg_obj is None:
                cfg_obj = ContextMemoryConfig(
                    base_dir=None,
                )
            cls._instance = ContextMemoryEngineV3(
                config=cfg_obj,
            )
            return cls._instance

        if isinstance(config, ContextMemoryConfig):
            cls._instance.apply_config(config)
        elif isinstance(config, dict):
            cls._instance.apply_config(ContextMemoryConfig.from_dict(config))

        return cls._instance


def get_context_memory(
        config: ContextMemoryConfig | dict[str, Any] | None = None,
        base_dir: str | Path | None = None,
        llm_preset: str = "",
        image_llm_preset: str = "",
        tool_presets: dict[str, str] | None = None,
        ask_timeout: float = 180.0,
        use_mock_llm: bool = False,
        enable_cleaning: bool = True,
        init_config: bool = True
):
    return ContextMemorySystem.get_instance(
        config=config,
        base_dir=base_dir,
        llm_preset=llm_preset,
        image_llm_preset=image_llm_preset,
        tool_presets=tool_presets,
        ask_timeout=ask_timeout,
        use_mock_llm=use_mock_llm,
        enable_cleaning=enable_cleaning,
        init_config=init_config
    )

