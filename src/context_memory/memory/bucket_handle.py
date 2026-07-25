from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, AsyncIterator, Literal, Protocol

from .models import (
    AddResult,
    BucketContextUsage,
    BucketInfo,
    CleanupResult,
    CompressResult,
    DeleteResult,
    EngineStats,
    GCResult,
    ListMemoriesResult,
    MemoryRecord,
    MoveResult,
    OptimizeResult,
    QueryResult,
    UpdateResult,
)
from .services import ADVANCE_QUERY_MODE_BEST_EFFORT
from context_memory.LLM_connect import Prompts, SystemPrompt, ToolInput

if TYPE_CHECKING:
    from .engine import ContextMemoryEngineV3


class BucketEngineProtocol(Protocol):
    async def resolve_bucket_handle_id(self, bucket_id: str) -> str: ...
    def get_bucket(self, bucket_id: str) -> "BucketHandle": ...
    async def _resolve_alias_from_resolved_bucket(
        self,
        bucket_id: str,
        alias: str,
        *,
        expected_type: str | None = None,
    ) -> str: ...
    async def _resolve_aliases_from_resolved_bucket(
        self,
        bucket_id: str,
        aliases: Iterable[str],
        *,
        expected_type: str | None = None,
        strict: bool = False,
    ) -> dict[str, str]: ...
    async def _iter_direct_records(self, bucket_id: str) -> AsyncIterator[MemoryRecord]: ...
    def _contains_direct_record(
        self,
        bucket_id: str,
        *,
        key_targets: set[str],
        bucket_targets: set[str],
    ) -> bool: ...
    def _resolve_bucket_id_for_handle(self, bucket_id: str) -> str: ...


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

    async def resolve_alias(self, alias: str, *, expected_type: str | None = None) -> str:
        """Resolve an alias through the current canonical bucket's alias map."""
        bucket_id = await self._refresh_bucket_id()
        return await self._engine._resolve_alias_from_resolved_bucket(
            bucket_id,
            alias,
            expected_type=expected_type,
        )

    async def resolve_aliases(
        self,
        aliases: Iterable[str],
        *,
        expected_type: str | None = None,
        strict: bool = False,
    ) -> dict[str, str]:
        """Resolve aliases after refreshing the canonical bucket once."""
        bucket_id = await self._refresh_bucket_id()
        return await self._engine._resolve_aliases_from_resolved_bucket(
            bucket_id,
            aliases,
            expected_type=expected_type,
            strict=strict,
        )

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
        top_k: int | None = None,
        use_cache: bool = True,
        mode: str = "auto",
        global_recall_top_n: int | None = None,
        global_recall_top_m: int | None = None,
        global_recall_depth_limit: int | None = None,
        global_recall_time_budget_ms: int | None = None,
        branch_expand_k: int | None = None,
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
            branch_expand_k=branch_expand_k,
        )

    async def advance_query(
        self,
        *,
        command: str = "",
        system_prompt: str | SystemPrompt | None = None,
        mode: Literal["single_shot", "best_effort_full_view"] = ADVANCE_QUERY_MODE_BEST_EFFORT,
        max_expand_depth: int | None = None,
        include_gray: bool = False,
        llm_preset: str | None = None,
        tool_input: ToolInput | list[ToolInput] | Prompts | None = None,
        enable_aliasing: bool = True,
        audit: bool = False,
        max_parallel_chunks: int | None = None,
    ) -> Any:
        bucket_id = await self._refresh_bucket_id()
        return await self._engine.advance_query(
            command=command,
            system_prompt=system_prompt,
            mode=mode,
            bucket_id=bucket_id,
            max_expand_depth=max_expand_depth,
            include_gray=include_gray,
            llm_preset=llm_preset,
            tool_input=tool_input,
            enable_aliasing=enable_aliasing,
            audit=audit,
            max_parallel_chunks=max_parallel_chunks,
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
        include_gray: bool = False,
        bucket_id: str | None = None,
    ) -> ListMemoriesResult:
        target_bucket = bucket_id or self.bucket_id
        result = await self._engine.list_memories(
            include_gray=include_gray,
            bucket_id=target_bucket,
        )
        if bucket_id is None:
            self.bucket_id = result.bucket_id
        return result

    async def get_bucket_context_usage(self, *, bucket_id: str | None = None) -> BucketContextUsage:
        target_bucket = bucket_id or self.bucket_id
        result = await self._engine.get_bucket_context_usage(bucket_id=target_bucket)
        if bucket_id is None:
            self.bucket_id = result.bucket_id
        return result

    async def migrate_storage_paths_to_relative(self) -> dict[str, int]:
        return await self._engine.migrate_storage_paths_to_relative()

    async def migration_status(self) -> dict[str, Any]:
        return await self._engine.migration_status()

    async def migrate_schema(self, *, dry_run: bool = False) -> dict[str, Any]:
        return await self._engine.migrate_schema(dry_run=dry_run)

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
        bucket_id = await self._refresh_bucket_id()
        async for record in self._engine._iter_direct_records(bucket_id):
            yield record

    def __contains__(self, item: object) -> bool:
        """Membership over direct bucket records, excluding gray by default.

        Supported item types:
        - `str`: memory key / bucket-node key / child bucket id
        - objects with `key` and/or `bucket_id`/`child_bucket_id` attributes
          (for example `MemoryRecord`, `BucketInfo`, `BucketHandle`)
        """
        eng = self._engine
        try:
            bucket_id = eng._resolve_bucket_id_for_handle(self.bucket_id)
            self.bucket_id = bucket_id
        except Exception:
            return False

        targets = self._contains_targets(item, eng)
        key_targets = targets["keys"]
        bucket_targets = targets["bucket_ids"]
        if not key_targets and not bucket_targets:
            return False

        return eng._contains_direct_record(
            bucket_id,
            key_targets=key_targets,
            bucket_targets=bucket_targets,
        )

    @staticmethod
    def _contains_targets(item: object, eng: BucketEngineProtocol) -> dict[str, set[str]]:
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
                text = eng._resolve_bucket_id_for_handle(text)
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
