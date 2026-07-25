from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal

from .bucket_handle import BucketHandle
from .config import (
    ContextMemoryConfig,
    _resolve_effective_max_context_window,
)
from .engine_runtime import EngineRuntime
from .services import (
    ADVANCE_QUERY_MODE_BEST_EFFORT,
    AdvanceQueryService,
    AliasService,
    BucketSplitService,
    BucketSummaryService,
    BucketTopologyService,
    CompressionService,
    ForgettingService,
    GovernanceService,
    IngestService,
    MaintenanceService,
    MemoryReadService,
    MigrationService,
    OptimizeService,
    QueryService,
    RecordPrimitivesService,
    RecordService,
    SplitIngestJobService,
)
from .version import __data_version__, __version__
from .models import (
    AddResult, BucketContextUsage, BucketInfo, CleanupResult, CompressResult,
    DeleteResult, EngineStats, GCResult, ListMemoriesResult, MemoryRecord,
    MoveResult, OptimizeResult, QueryResult, UpdateResult,
)
from context_memory.LLM_connect import Prompts, SystemPrompt, ToolInput

__all__ = [
    "ADVANCE_QUERY_MODE_BEST_EFFORT",
    "BucketHandle",
    "ContextMemoryConfig",
    "ContextMemoryEngineV3",
    "ContextMemorySystem",
    "__data_version__",
    "__version__",
    "get_context_memory_engine",
]


class ContextMemoryEngineV3:
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
    ) -> None:
        self._runtime = EngineRuntime(base_dir, config=config, llm_preset=llm_preset, image_llm_preset=image_llm_preset, tool_presets=tool_presets, ask_timeout=ask_timeout, auto_resume_pending_jobs=auto_resume_pending_jobs, use_mock_llm=use_mock_llm, enable_cleaning=enable_cleaning, init_config=init_config, evidence_versions=evidence_versions, auto_manage=auto_manage, enable_forgetting=enable_forgetting, max_bucket_depth=max_bucket_depth, max_memory_bytes=max_memory_bytes, auto_compress_trigger_ratio=auto_compress_trigger_ratio, auto_split_trigger_ratio=auto_split_trigger_ratio, split_plan_target_items=split_plan_target_items, split_plan_hard_cap=split_plan_hard_cap, auto_split_cooldown_sec=auto_split_cooldown_sec, auto_split_min_drop_abs=auto_split_min_drop_abs, auto_split_max_round_per_manage=auto_split_max_round_per_manage, split_ingest_parallelism=split_ingest_parallelism, split_ingest_delay_min=split_ingest_delay_min, split_ingest_delay_max=split_ingest_delay_max, optimize_leaf_loss_threshold=optimize_leaf_loss_threshold, gc_revision_retention_days=gc_revision_retention_days, gc_gray_key_retention_days=gc_gray_key_retention_days, gc_archived_bucket_retention_days=gc_archived_bucket_retention_days, query_top_k_default=query_top_k_default, query_branch_expand_k=query_branch_expand_k, query_branch_expand_bind_top_k=query_branch_expand_bind_top_k, query_max_depth_default=query_max_depth_default, query_mode_default=query_mode_default, global_recall_top_n=global_recall_top_n, global_recall_top_m=global_recall_top_m, global_recall_depth_limit=global_recall_depth_limit, global_recall_time_budget_ms=global_recall_time_budget_ms, global_recall_boost_weight=global_recall_boost_weight, data_version=__data_version__)
        self._runtime.max_context_window = _resolve_effective_max_context_window(
            self._runtime.llm_preset
        )
        services: dict[str, Any] = {}

        def provider(name: str):
            return lambda: services[name]

        services["migration"] = MigrationService(
            self._runtime,
            code_schema_version=lambda: __data_version__,
            engine_version=lambda: __version__,
            topology=provider("topology"),
        )
        services["alias"] = AliasService(
            self._runtime,
            topology=provider("topology"),
        )
        services["primitives"] = RecordPrimitivesService(
            self._runtime,
            alias=provider("alias"),
        )
        services["maintenance"] = MaintenanceService(
            self._runtime,
            record=provider("record"),
            topology=provider("topology"),
        )
        services["topology"] = BucketTopologyService(
            self._runtime,
            alias=provider("alias"),
            handle_factory=lambda bucket_id: BucketHandle(self, bucket_id),
            maintenance=provider("maintenance"),
            primitives=provider("primitives"),
        )
        services["summary"] = BucketSummaryService(
            self._runtime,
            alias=provider("alias"),
            maintenance=provider("maintenance"),
            primitives=provider("primitives"),
            topology=provider("topology"),
        )
        services["forgetting"] = ForgettingService(
            self._runtime,
            primitives=provider("primitives"),
        )
        services["read"] = MemoryReadService(
            self._runtime,
            topology=provider("topology"),
        )
        services["split"] = BucketSplitService(
            self._runtime,
            alias=provider("alias"),
            governance=provider("governance"),
            maintenance=provider("maintenance"),
            primitives=provider("primitives"),
            summary=provider("summary"),
            topology=provider("topology"),
        )
        services["compression"] = CompressionService(
            self._runtime,
            alias=provider("alias"),
            forgetting=provider("forgetting"),
            maintenance=provider("maintenance"),
            primitives=provider("primitives"),
            split=provider("split"),
            summary=provider("summary"),
            topology=provider("topology"),
        )
        services["governance"] = GovernanceService(
            self._runtime,
            compression=provider("compression"),
            forgetting=provider("forgetting"),
            read=provider("read"),
            split=provider("split"),
            topology=provider("topology"),
        )
        services["optimize"] = OptimizeService(
            self._runtime,
            alias=provider("alias"),
            compression=provider("compression"),
            governance=provider("governance"),
            maintenance=provider("maintenance"),
            primitives=provider("primitives"),
            summary=provider("summary"),
            topology=provider("topology"),
        )
        services["ingest"] = IngestService(
            self._runtime,
            alias=provider("alias"),
            compression=provider("compression"),
            governance=provider("governance"),
            maintenance=provider("maintenance"),
            optimize=provider("optimize"),
            primitives=provider("primitives"),
            read=provider("read"),
            summary=provider("summary"),
            topology=provider("topology"),
        )
        services["jobs"] = SplitIngestJobService(
            self._runtime,
            alias=provider("alias"),
            compression=provider("compression"),
            governance=provider("governance"),
            ingest=provider("ingest"),
            maintenance=provider("maintenance"),
            primitives=provider("primitives"),
            topology=provider("topology"),
        )
        services["record"] = RecordService(
            self._runtime,
            alias=provider("alias"),
            governance=provider("governance"),
            ingest=provider("ingest"),
            maintenance=provider("maintenance"),
            primitives=provider("primitives"),
            topology=provider("topology"),
        )
        services["query"] = QueryService(
            self._runtime,
            alias=provider("alias"),
            forgetting=provider("forgetting"),
            topology=provider("topology"),
        )
        services["advance"] = AdvanceQueryService(
            self._runtime,
            alias=provider("alias"),
            topology=provider("topology"),
        )
        self._services = services
        self._migration = services["migration"]
        self._alias = services["alias"]
        self._topology = services["topology"]
        self._summary = services["summary"]
        self._ingest = services["ingest"]
        self._jobs = services["jobs"]
        self._record = services["record"]
        self._query = services["query"]
        self._advance = services["advance"]
        self._compression = services["compression"]
        self._split = services["split"]
        self._optimize = services["optimize"]
        self._maintenance = services["maintenance"]
        self._read = services["read"]
        if self._runtime.base_dir is not None:
            self._migration._migrate_if_needed(force=False, dry_run=False)
        self._jobs._trigger_auto_resume_pending_jobs()

    @property
    def storage(self):
        return self._runtime.storage

    @property
    def pipeline(self):
        return self._runtime.pipeline

    @property
    def token_counter(self):
        return self._runtime.token_counter

    @property
    def memory_manager(self):
        return self._runtime.memory_manager

    @property
    def bm25_cache(self):
        return self._runtime.bm25_cache

    @property
    def alias_codec(self):
        return self._runtime.alias_codec

    @property
    def image_extractor(self):
        return self._runtime.image_extractor

    @property
    def max_context_window(self) -> int:
        return self._runtime.max_context_window

    @property
    def llm_preset(self) -> str:
        return self._runtime.llm_preset

    @property
    def image_llm_preset(self) -> str:
        return self._runtime.image_llm_preset

    @property
    def tool_presets(self) -> dict[str, str]:
        return dict(self._runtime.tool_presets)

    def apply_config(self, config: ContextMemoryConfig | dict[str, Any]) -> None:
        self._runtime.apply_config(config)
        self._runtime.max_context_window = _resolve_effective_max_context_window(
            self._runtime.llm_preset
        )
        if self._runtime.base_dir is not None:
            self._migration._migrate_if_needed(force=False, dry_run=False)
        self._jobs._trigger_auto_resume_pending_jobs()

    def root_bucket_id(self) -> str:
        return self._topology.root_bucket_id()

    def active_bucket_id(self) -> str:
        return self._topology.active_bucket_id()

    @property
    def bucket_id(self):
        return self._topology.bucket_id()

    async def set_active_bucket(self, bucket_id: str) -> dict[str, Any]:
        return await self._topology.set_active_bucket(bucket_id)

    async def switch_active_bucket(self, bucket_id: str) -> dict[str, Any]:
        return await self._topology.switch_active_bucket(bucket_id)

    async def resolve_bucket_handle_id(self, bucket_id: str) -> str:
        return await self._topology.resolve_bucket_handle_id(bucket_id)

    async def latest_bucket_id(self, bucket_id: str | None = None) -> str:
        return await self._topology.latest_bucket_id(bucket_id)

    def get_bucket(self, bucket_id: str) -> BucketHandle:
        canonical, _ = self._topology._resolve_bucket_redirect_chain(bucket_id)
        return BucketHandle(self, canonical)

    def list_buckets(self) -> list[BucketInfo]:
        return self._topology.list_buckets()

    def get_or_create_alias(self, bucket_id: str, real_key: str, key_type: str) -> str:
        return self._alias.get_or_create_alias(bucket_id, real_key, key_type)

    def resolve_alias(self, bucket_id: str, alias: str, expected_type: str | None = None) -> str:
        return self._alias.resolve_alias(bucket_id, alias, expected_type)

    async def resolve_aliases(
        self,
        bucket_id: str,
        aliases: Iterable[str],
        *,
        expected_type: str | None = None,
        strict: bool = False,
    ) -> dict[str, str]:
        return await self._alias.resolve_aliases(bucket_id, aliases, expected_type=expected_type, strict=strict)

    def freeze_alias_map(self, bucket_id: str) -> None:
        return self._alias.freeze_alias_map(bucket_id)

    def alias_map_version(self, bucket_id: str) -> int:
        return self._alias.alias_map_version(bucket_id)

    def build_llm_view(
        self,
        bucket_id: str,
        real_payload: Any,
        map_version: int | None = None,
        *,
        allow_create: bool = True,
    ) -> Any:
        return self._alias.build_llm_view(bucket_id, real_payload, map_version, allow_create=allow_create)

    async def prepare_alias_payload(
        self,
        bucket_id: str,
        real_payload: Any,
        map_version: int | None = None,
        *,
        allow_create: bool = True,
    ) -> Any:
        return await self._alias.prepare_alias_payload(bucket_id, real_payload, map_version, allow_create=allow_create)

    async def restore_alias_payload(
        self,
        bucket_id: str,
        alias_payload: Any,
        map_version: int | None = None,
        *,
        strict_unknown: bool = True,
    ) -> Any:
        return await self._alias.restore_alias_payload(bucket_id, alias_payload, map_version, strict_unknown=strict_unknown)

    def resolve_llm_output(self, bucket_id: str, alias_output: Any, map_version: int | None = None) -> Any:
        return self._alias.resolve_llm_output(bucket_id, alias_output, map_version)

    def assert_alias_only_payload(self, bucket_id: str, payload: Any) -> None:
        return self._alias.assert_alias_only_payload(bucket_id, payload)

    def shutdown(self, *, wait: bool = False) -> None:
        self._query.shutdown()
        return self._runtime.shutdown(wait=wait)

    async def close(self, *, wait: bool = False) -> None:
        await self._query.close()
        return await self._runtime.close(wait=wait)

    async def set_bucket_with_id(
            self,
            title: str,
            parent_bucket_id: str,
            *,
            summary: str = "",
            content: str = "",
            summary_locked: bool = False
    ) -> BucketHandle:
        return await self._topology.set_bucket_with_id(title, parent_bucket_id, summary=summary, content=content, summary_locked=summary_locked)

    async def set_bucket(
            self,
            title: str,
            *,
            summary: str = "",
            content: str = "",
            summary_locked: bool = False
    ) -> BucketHandle:
        return await self._topology.set_bucket(title, summary=summary, content=content, summary_locked=summary_locked)

    async def create_bucket(
        self,
        parent_bucket_id: str,
        *,
        title: str,
        summary: str = "",
        content: str = "",
        summary_locked: bool = False,
    ) -> BucketInfo:
        return await self._topology.create_bucket(parent_bucket_id, title=title, summary=summary, content=content, summary_locked=summary_locked)

    async def create_child_bucket(
        self,
        parent_bucket_id: str | None = None,
        *,
        title: str,
        summary: str = "",
        content: str = "",
        summary_locked: bool = False,
    ) -> BucketInfo:
        return await self._topology.create_child_bucket(parent_bucket_id, title=title, summary=summary, content=content, summary_locked=summary_locked)

    async def refresh_bucket_summary(self, bucket_id: str, *, force: bool = False) -> dict[str, Any]:
        return await self._summary.refresh_bucket_summary(bucket_id, force=force)

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
        return await self._ingest.add_memory(raw_text, evidence_path=evidence_path, key=key, topic=topic, bucket_id=bucket_id, force_split=force_split, create_new_bucket=create_new_bucket, chunk_max_chars=chunk_max_chars, chunk_overlap_chars=chunk_overlap_chars, dedup_in_bucket=dedup_in_bucket)

    async def resume_pending_jobs(self) -> dict[str, Any]:
        return await self._jobs.resume_pending_jobs()

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
        return await self._ingest.add_memory_from_file(file_path, topic=topic, bucket_id=bucket_id, image_extract_hint=image_extract_hint, query_hint=query_hint, force_split=force_split, create_new_bucket=create_new_bucket, chunk_max_chars=chunk_max_chars, chunk_overlap_chars=chunk_overlap_chars, dedup_in_bucket=dedup_in_bucket, auto_optimize_after_split=auto_optimize_after_split)

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
        return await self._ingest.add_memory_from_dir(dir_path, bucket_id=bucket_id, auto_create_sub_buckets=auto_create_sub_buckets, image_extract_hint=image_extract_hint, force_split=force_split, create_new_bucket=create_new_bucket, chunk_max_chars=chunk_max_chars, chunk_overlap_chars=chunk_overlap_chars, dedup_in_bucket=dedup_in_bucket, collect_token_usage=collect_token_usage)

    async def get_memory(
        self,
        key: str,
        *,
        with_evidence: bool = False,
        revision: str | None = None,
    ) -> MemoryRecord | None:
        return await self._record.get_memory(key, with_evidence=with_evidence, revision=revision)

    async def export_memory_to_markdown(self, memory_id: str) -> dict[str, Any]:
        return await self._record.export_memory_to_markdown(memory_id)

    async def get_evidence_content(self, key: str, *, revision: str | None = None) -> str:
        return await self._record.get_evidence_content(key, revision=revision)

    async def list_memories(
        self,
        *,
        include_gray: bool = False,
        bucket_id: str | None = None,
    ) -> ListMemoriesResult:
        return await self._read.list_memories(include_gray=include_gray, bucket_id=bucket_id)

    async def get_bucket_context_usage(self, bucket_id: str | None = None) -> BucketContextUsage:
        return await self._read.get_context_usage(
            bucket_id,
            allow_fallback=True,
        )

    async def update_memory(
        self,
        key: str,
        patch_text: str,
        *,
        evidence_path: str | None = None,
    ) -> UpdateResult:
        return await self._record.update_memory(key, patch_text, evidence_path=evidence_path)

    async def set_gray(self, key: str, *, gray: bool, reason: str = "manual") -> UpdateResult:
        return await self._record.set_gray(key, gray=gray, reason=reason)

    async def delete_memory(self, key: Any, *, reason: str = "") -> DeleteResult:
        return await self._record.delete_memory(key, reason=reason)

    async def query(
        self,
        query_text: str,
        *,
        top_k: int | None = None,
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
        branch_expand_k: int | None = None,
    ) -> QueryResult:
        self._query.ensure_side_effect_worker()
        normalized_mode = self._runtime.normalize_query_mode_value(
            mode,
            field_name="mode",
        )
        effective_top_k = max(
            1,
            int(top_k if top_k is not None else self._runtime._query_top_k_default),
        )
        return await self._query.run_query(
            query_text,
            top_k=effective_top_k,
            include_gray=include_gray,
            with_evidence=with_evidence,
            use_cache=use_cache,
            bucket_id=bucket_id,
            max_depth=max_depth,
            mode=normalized_mode,
            global_recall_top_n=global_recall_top_n,
            global_recall_top_m=global_recall_top_m,
            global_recall_depth_limit=global_recall_depth_limit,
            global_recall_time_budget_ms=global_recall_time_budget_ms,
            branch_expand_k=branch_expand_k,
        )

    async def advance_query(
        self,
        *,
        command: str = "",
        system_prompt: str | SystemPrompt | None = None,
        mode: Literal["single_shot", "best_effort_full_view"] = ADVANCE_QUERY_MODE_BEST_EFFORT,
        bucket_id: str | None = None,
        max_expand_depth: int | None = None,
        include_gray: bool = False,
        llm_preset: str | None = None,
        tool_input: ToolInput | list[ToolInput] | Prompts | None = None,
        enable_aliasing: bool = True,
        audit: bool = False,
        max_parallel_chunks: int | None = None,
    ) -> Any:
        return await self._advance.advance_query(command=command, system_prompt=system_prompt, mode=mode, bucket_id=bucket_id, max_expand_depth=max_expand_depth, include_gray=include_gray, llm_preset=llm_preset, tool_input=tool_input, enable_aliasing=enable_aliasing, audit=audit, max_parallel_chunks=max_parallel_chunks)

    async def force_compress(self, *, reason: str = "manual", bucket_id: str | None = None) -> CompressResult:
        return await self._compression.force_compress(reason=reason, bucket_id=bucket_id)

    async def split_bucket(
        self,
        bucket_id: str,
        *,
        reason: str = "manual_split",
        target_groups_min: int = 2,
        target_groups_max: int = 10,
    ) -> dict[str, Any]:
        return await self._split.split_bucket(bucket_id, reason=reason, target_groups_min=target_groups_min, target_groups_max=target_groups_max)

    async def optimize(
        self,
        *,
        bucket_id: str | None = None,
        reason: str = "manual_optimize",
    ) -> OptimizeResult:
        return await self._optimize.optimize(bucket_id=bucket_id, reason=reason)

    async def move_item(
        self,
        key: str,
        *,
        target_bucket_id: str,
        reason: str = "manual_move",
    ) -> MoveResult:
        return await self._record.move_item(key, target_bucket_id=target_bucket_id, reason=reason)

    async def gc_storage(self, *, dry_run: bool = True, reason: str = "manual_gc") -> GCResult:
        return await self._maintenance.gc_storage(dry_run=dry_run, reason=reason)

    async def cleanup_expired(self) -> CleanupResult:
        return await self._maintenance.cleanup_expired()

    async def stats(self) -> EngineStats:
        return await self._maintenance.stats()

    async def migration_status(self) -> dict[str, Any]:
        return await self._migration.migration_status()

    async def migrate_schema(self, *, dry_run: bool = False) -> dict[str, Any]:
        return await self._migration.migrate_schema(dry_run=dry_run)

    async def migrate_storage_paths_to_relative(self) -> dict[str, int]:
        return await self._migration.migrate_storage_paths_to_relative()

    async def _resolve_alias_from_resolved_bucket(
        self,
        bucket_id: str,
        alias: str,
        *,
        expected_type: str | None = None,
    ) -> str:
        return await self._alias._resolve_alias_from_resolved_bucket(
            bucket_id,
            alias,
            expected_type=expected_type,
        )

    async def _resolve_aliases_from_resolved_bucket(
        self,
        bucket_id: str,
        aliases: Iterable[str],
        *,
        expected_type: str | None = None,
        strict: bool = False,
    ) -> dict[str, str]:
        return await self._alias._resolve_aliases_from_resolved_bucket(
            bucket_id,
            aliases,
            expected_type=expected_type,
            strict=strict,
        )

    async def _iter_direct_records(self, bucket_id: str):
        async for record in self._read.iter_direct_records(
            bucket_id,
            include_gray=False,
        ):
            yield record

    def _contains_direct_record(
        self,
        bucket_id: str,
        *,
        key_targets: set[str],
        bucket_targets: set[str],
    ) -> bool:
        return self._read.contains_direct_record(
            bucket_id,
            key_targets=key_targets,
            bucket_targets=bucket_targets,
        )

    def _resolve_bucket_id_for_handle(self, bucket_id: str) -> str:
        return self._topology._resolve_bucket_id(bucket_id)


from .factory import ContextMemorySystem, get_context_memory_engine
