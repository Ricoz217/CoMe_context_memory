from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from ..models import (
    BUCKET_KIND_BUCKET,
    BUCKET_KIND_MEMORY,
    BucketContextUsage,
    ListMemoriesResult,
    MemoryIndexItem,
    MemoryRecord,
)
from ..token_counter import TokenCountError
from .runtime import ServiceRuntime

if TYPE_CHECKING:
    from ..engine import ContextMemoryEngineV3


class MemoryReadService:
    """Index-backed reads used by BucketHandle protocols."""

    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    @property
    def engine(self) -> "ContextMemoryEngineV3":
        return self.runtime.engine

    async def list_memories(
        self,
        bucket_id: str | None,
        *,
        include_gray: bool,
    ) -> ListMemoriesResult:
        tree = await asyncio.to_thread(self.engine.storage.load_bucket_tree)
        resolved_bucket_id = self._resolve_requested_bucket(bucket_id, tree)
        index_task = asyncio.to_thread(
            self._build_index_result,
            resolved_bucket_id,
            include_gray,
            tree,
        )
        usage_task = self.get_context_usage(
            resolved_bucket_id,
            allow_fallback=True,
            resolve_bucket=False,
        )
        index_result, usage = await asyncio.gather(index_task, usage_task)
        memories, buckets, total_memory_count = index_result
        return ListMemoriesResult(
            bucket_id=resolved_bucket_id,
            memories=memories,
            buckets=buckets,
            memory_count=len(memories),
            total_memory_count=total_memory_count,
            bucket_count=len(buckets),
            context_tokens=usage.context_tokens,
            max_context_window=usage.max_context_window,
            usage_ratio=usage.usage_ratio,
            include_gray=include_gray,
            token_count_method=usage.token_count_method,
        )

    async def get_context_usage(
        self,
        bucket_id: str | None,
        *,
        allow_fallback: bool,
        resolve_bucket: bool = True,
    ) -> BucketContextUsage:
        eng = self.engine
        if resolve_bucket:
            tree = await asyncio.to_thread(eng.storage.load_bucket_tree)
            bucket_id = self._resolve_requested_bucket(bucket_id, tree)
        else:
            bucket_id = str(bucket_id or "").strip()
        bucket_version = await asyncio.to_thread(eng.storage.get_bucket_version, bucket_id)
        cache_key = f"ctx_tokens:{bucket_id}"
        cached = eng.memory_manager.get(cache_key)
        if isinstance(cached, dict) and int(cached.get("version", -1)) == bucket_version:
            method = str(cached.get("method", ""))
            if method == "tiktoken" or (allow_fallback and method == "char_estimate"):
                return self._make_context_usage(
                    bucket_id,
                    int(cached.get("tokens", 1)),
                    method,
                )

        token_count = await asyncio.to_thread(
            self._load_and_count_context,
            bucket_id,
            allow_fallback,
        )
        eng.memory_manager.set(
            cache_key,
            {
                "version": bucket_version,
                "tokens": token_count[0],
                "method": token_count[1],
            },
            bytes_estimate=160,
            dirty=False,
        )
        return self._make_context_usage(bucket_id, token_count[0], token_count[1])

    async def count_direct_records(self, bucket_id: str, *, include_gray: bool) -> int:
        return await asyncio.to_thread(self._count_direct_records, bucket_id, include_gray)

    def _build_index_result(
        self,
        bucket_id: str,
        include_gray: bool,
        tree: dict[str, Any],
    ) -> tuple[list[MemoryIndexItem], list[MemoryIndexItem], int]:
        state = self.engine.storage.load_state()
        keys = state.get("keys", {})
        if not isinstance(keys, dict):
            return [], [], 0

        records_by_bucket: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for key, node in keys.items():
            if not isinstance(node, dict):
                continue
            if not include_gray and bool(node.get("gray", False)):
                continue
            owner = str(node.get("bucket_id", ""))
            records_by_bucket.setdefault(owner, []).append((str(key), node))

        direct = records_by_bucket.get(bucket_id, [])
        memories = [self._index_item(key, node) for key, node in direct if node.get("kind") == BUCKET_KIND_MEMORY]
        buckets = [self._index_item(key, node) for key, node in direct if node.get("kind") == BUCKET_KIND_BUCKET]

        total = 0
        visited: set[str] = set()
        stack = [bucket_id]
        while stack:
            current = self._resolve_bucket_from_tree(stack.pop(), tree)
            if not current or current in visited:
                continue
            visited.add(current)
            for _, node in records_by_bucket.get(current, []):
                if node.get("kind") == BUCKET_KIND_MEMORY:
                    total += 1
                elif node.get("kind") == BUCKET_KIND_BUCKET:
                    child = self._resolve_bucket_from_tree(str(node.get("child_bucket_id", "")), tree)
                    if child and child not in visited:
                        stack.append(child)
        return memories, buckets, total

    @classmethod
    def _resolve_requested_bucket(cls, bucket_id: str | None, tree: dict[str, Any]) -> str:
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            raise ValueError("bucket tree buckets is invalid")
        raw = str(bucket_id or "").strip()
        if not raw:
            raw = str(tree.get("active_bucket_id", "") or tree.get("root_bucket_id", "")).strip()
        resolved = cls._resolve_bucket_from_tree(raw, tree)
        if not resolved or resolved not in buckets:
            raise ValueError(f"bucket not found: {raw}")
        return resolved

    def _count_direct_records(self, bucket_id: str, include_gray: bool) -> int:
        state = self.engine.storage.load_state()
        keys = state.get("keys", {})
        if not isinstance(keys, dict):
            return 0
        return sum(
            1
            for node in keys.values()
            if self._matches_direct_node(node, bucket_id=bucket_id, include_gray=include_gray)
        )

    def _load_and_count_context(self, bucket_id: str, allow_fallback: bool) -> tuple[int, str]:
        context = self.engine.storage.load_bucket_context(bucket_id)
        try:
            prompts = context.to_prompts()
        except Exception:
            prompts = []
        raw_text = "\n".join(
            prompt.text
            for prompt in prompts or []
            if isinstance(getattr(prompt, "text", None), str) and prompt.text
        )
        if allow_fallback:
            count = self.engine.token_counter.count_text_for_display(raw_text)
            return count.tokens, count.method
        try:
            return self.engine.token_counter.count_text(raw_text), "tiktoken"
        except TokenCountError:
            raise

    def _make_context_usage(self, bucket_id: str, tokens: int, method: str) -> BucketContextUsage:
        max_window = max(1, int(self.engine.max_context_window))
        normalized_method = "char_estimate" if method == "char_estimate" else "tiktoken"
        return BucketContextUsage(
            bucket_id=bucket_id,
            context_tokens=max(1, int(tokens)),
            max_context_window=max_window,
            usage_ratio=max(0.0, min(1.0, float(tokens) / float(max_window))),
            token_count_method=normalized_method,
        )

    @staticmethod
    def _index_item(key: str, node: dict[str, Any]) -> MemoryIndexItem:
        kind = BUCKET_KIND_BUCKET if node.get("kind") == BUCKET_KIND_BUCKET else BUCKET_KIND_MEMORY
        return MemoryIndexItem(
            key=key,
            revision_id=str(node.get("latest_revision", "")),
            kind=kind,
            bucket_id=str(node.get("bucket_id", "")),
            child_bucket_id=str(node.get("child_bucket_id", "") or ""),
            gray=bool(node.get("gray", False)),
            updated_at=str(node.get("updated_at", "")),
        )

    @staticmethod
    def _resolve_bucket_from_tree(bucket_id: str, tree: dict[str, Any]) -> str:
        current = str(bucket_id or "").strip()
        buckets = tree.get("buckets", {})
        if not current or not isinstance(buckets, dict):
            return current
        visited: set[str] = set()
        while current and current not in visited:
            visited.add(current)
            raw = buckets.get(current)
            if not isinstance(raw, dict) or not bool(raw.get("sealed", False)):
                break
            successor = str(raw.get("sealed_to", "") or "").strip()
            if not successor or successor not in buckets:
                break
            current = successor
        return current

    async def iter_direct_records(
        self,
        bucket_id: str,
        *,
        include_gray: bool = False,
    ) -> AsyncIterator[MemoryRecord]:
        storage = self.engine.storage
        state = await asyncio.to_thread(storage.load_state)
        keys = state.get("keys", {})
        if not isinstance(keys, dict):
            return

        # Preserve BucketHandle's existing memory-first, bucket-node-second order.
        for expected_kind in (BUCKET_KIND_MEMORY, BUCKET_KIND_BUCKET):
            for node in keys.values():
                if not self._matches_direct_node(
                    node,
                    bucket_id=bucket_id,
                    expected_kind=expected_kind,
                    include_gray=include_gray,
                ):
                    continue
                record = await asyncio.to_thread(storage.load_record_from_index_node, node)
                if record is not None:
                    yield record

    def contains_direct_record(
        self,
        bucket_id: str,
        *,
        key_targets: set[str],
        bucket_targets: set[str],
    ) -> bool:
        keys = self.engine.storage.load_state().get("keys", {})
        if not isinstance(keys, dict):
            return False

        for key, node in keys.items():
            if not self._matches_direct_node(
                node,
                bucket_id=bucket_id,
                include_gray=False,
            ):
                continue
            if str(key) in key_targets:
                return True
            if not bucket_targets or node.get("kind") != BUCKET_KIND_BUCKET:
                continue

            child_raw = str(node.get("child_bucket_id", "") or "").strip()
            if not child_raw:
                continue
            try:
                child_bucket = self.engine._resolve_bucket_id(child_raw)
            except Exception:
                child_bucket = child_raw
            if child_bucket in bucket_targets:
                return True
        return False

    @staticmethod
    def _matches_direct_node(
        node: Any,
        *,
        bucket_id: str,
        expected_kind: str | None = None,
        include_gray: bool,
    ) -> bool:
        if not isinstance(node, dict):
            return False
        if str(node.get("bucket_id", "")) != bucket_id:
            return False
        if expected_kind is not None and node.get("kind") != expected_kind:
            return False
        return include_gray or not bool(node.get("gray", False))
