from __future__ import annotations

import asyncio
import json
from typing import Any, Literal

from context_memory.LLM_connect import Chat, Prompts, SystemPrompt, TextPrompt, ToolInput, parse_llm_setting

from ..models import BUCKET_KIND_BUCKET, BUCKET_KIND_MEMORY, BucketInfo, MemoryRecord
from .runtime import ServiceRuntime


ADVANCE_QUERY_MODE_SINGLE_SHOT = "single_shot"
ADVANCE_QUERY_MODE_BEST_EFFORT = "best_effort_full_view"
ADVANCE_QUERY_OVERFLOW_RATIO = 0.80
ADVANCE_QUERY_DEFAULT_SYSTEM_PROMPT = (
    "You are an assistant for full-memory analysis. "
    "Follow the user command exactly and use the provided memory payload as the primary evidence."
)


class AdvanceQueryService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

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
    ) -> Prompts:
        eng = self.runtime.engine
        mode_value = self._normalize_advance_query_mode(mode)
        target_bucket_id = str(bucket_id or "").strip()
        system_text = self._advance_resolve_system_prompt_text(system_prompt)
        threshold_tokens = max(1, int(eng.max_context_window * ADVANCE_QUERY_OVERFLOW_RATIO))
        parallel_limit = max(
            1,
            int(max_parallel_chunks if max_parallel_chunks is not None else eng._split_ingest_parallelism),
        )
        if max_expand_depth is not None and int(max_expand_depth) < 0:
            raise ValueError("max_expand_depth must be >= 0 or None")

        eng._begin_alias_session()
        try:
            target_bucket_id, root_node, full_payload = await self._advance_collect_bucket_snapshot(
                bucket_id=target_bucket_id,
                include_gray=bool(include_gray),
                max_expand_depth=(None if max_expand_depth is None else int(max_expand_depth)),
            )
            prepared_system, prepared_command, prepared_full_payload = await self._advance_prepare_components_for_llm(
                raw_payload=full_payload,
                alias_bucket_id=target_bucket_id,
                enable_aliasing=bool(enable_aliasing),
                system_text=system_text,
                command=command,
            )
            prepared_user_markdown = self._advance_build_user_markdown(
                command=prepared_command,
                payload=prepared_full_payload,
            )
            await self._advance_assert_request_safe(
                alias_bucket_id=target_bucket_id,
                enable_aliasing=bool(enable_aliasing),
                system_text=prepared_system,
                user_markdown=prepared_user_markdown,
            )
            token_count = await self._advance_count_tokens_exact(
                self._advance_combine_request_markdown(
                    system_text=prepared_system,
                    user_markdown=prepared_user_markdown,
                )
            )
            await self._advance_audit_event(
                enabled=bool(audit),
                event_type="ADVANCE_QUERY_START",
                bucket_id=target_bucket_id,
                payload={
                    "mode": mode_value,
                    "threshold_tokens": threshold_tokens,
                    "payload_tokens": int(token_count),
                    "include_gray": bool(include_gray),
                    "max_expand_depth": max_expand_depth,
                    "enable_aliasing": bool(enable_aliasing),
                },
            )

            if mode_value == ADVANCE_QUERY_MODE_SINGLE_SHOT:
                if token_count > threshold_tokens:
                    raise RuntimeError(
                        f"advance_query single_shot overflow: tokens={token_count}, threshold={threshold_tokens}; "
                        "please compress/split bucket or use best_effort_full_view."
                    )
                return await self._advance_llm_request(
                    system_text=prepared_system,
                    user_markdown=prepared_user_markdown,
                    llm_preset=llm_preset,
                    tool_input=tool_input,
                    allow_tools=True,
                    alias_bucket_id=target_bucket_id if enable_aliasing else None,
                )

            response = await self._advance_run_best_effort_node(
                node=root_node,
                command=prepared_command,
                system_text=prepared_system,
                llm_preset=llm_preset,
                threshold_tokens=threshold_tokens,
                alias_bucket_id=target_bucket_id,
                enable_aliasing=bool(enable_aliasing),
                parallel_limit=parallel_limit,
                audit=bool(audit),
                allow_tools_on_final=True,
                final_tool_input=tool_input,
            )
            await self._advance_audit_event(
                enabled=bool(audit),
                event_type="ADVANCE_QUERY_SUCCESS",
                bucket_id=target_bucket_id,
                payload={"mode": mode_value, "threshold_tokens": threshold_tokens},
            )
            return response
        except Exception as exc:
            await self._advance_audit_event(
                enabled=bool(audit),
                event_type="ADVANCE_QUERY_FAIL",
                bucket_id=target_bucket_id,
                payload={"mode": mode_value, "error": repr(exc)},
            )
            raise
        finally:
            eng._end_alias_session(flush=True)

    def _normalize_advance_query_mode(self, mode: str) -> str:
        token = str(mode or "").strip().lower()
        if token in {"single", "single_shot", "single-shot"}:
            return ADVANCE_QUERY_MODE_SINGLE_SHOT
        if token in {"best_effort", "best_effort_full_view", "best-effort", "best-effort-full-view"}:
            return ADVANCE_QUERY_MODE_BEST_EFFORT
        raise ValueError("invalid advance_query mode; expected single_shot or best_effort_full_view")

    @staticmethod
    def _advance_resolve_system_prompt_text(system_prompt: str | SystemPrompt | None) -> str:
        if isinstance(system_prompt, SystemPrompt):
            text = str(system_prompt.text or "").strip()
            return text or ADVANCE_QUERY_DEFAULT_SYSTEM_PROMPT
        text = str(system_prompt or "").strip()
        return text or ADVANCE_QUERY_DEFAULT_SYSTEM_PROMPT

    @staticmethod
    def _advance_memory_metadata(rec: MemoryRecord) -> dict[str, Any]:
        return {
            "key": rec.key,
            "revision_id": rec.revision_id,
            "bucket_id": rec.bucket_id,
            "title": rec.title,
            "summary": rec.summary,
            "weight": float(rec.weight),
            "event": rec.event,
            "created_at": rec.created_at,
            "expires_at": rec.expires_at,
            "confidence_type": str(rec.confidence_type or "common"),
        }

    @staticmethod
    def _advance_bucket_metadata(info: BucketInfo) -> dict[str, Any]:
        return {
            "bucket_id": info.bucket_id,
            "parent_bucket_id": info.parent_bucket_id,
            "level": int(info.level),
            "node_key": info.node_key,
            "title": info.title,
            "summary": info.summary,
            "last_event_at": float(info.last_event_at or 0.0),
            "updated_at": info.updated_at,
        }

    def _advance_collect_bucket_tree(
        self,
        *,
        bucket_id: str,
        include_gray: bool,
        max_expand_depth: int | None,
        depth: int,
        visited: set[str],
    ) -> dict[str, Any]:
        """Compatibility facade for callers that still need the synchronous collector."""
        eng = self.runtime.engine
        tree = eng.storage.topology_snapshot()
        resolved_bucket_id = self._advance_resolve_bucket_from_tree(bucket_id, tree)
        records_by_bucket = self._advance_collect_subtree_index_nodes(
            bucket_id=resolved_bucket_id,
            include_gray=include_gray,
            max_expand_depth=max_expand_depth,
            tree=tree,
        )
        return self._advance_collect_bucket_tree_from_snapshot(
            bucket_id=resolved_bucket_id,
            include_gray=include_gray,
            max_expand_depth=max_expand_depth,
            depth=depth,
            visited=visited,
            tree=tree,
            records_by_bucket=records_by_bucket,
        )

    async def _advance_collect_bucket_snapshot(
        self,
        *,
        bucket_id: str | None,
        include_gray: bool,
        max_expand_depth: int | None,
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        return await self.runtime.engine._run_storage_task(
            self._advance_collect_bucket_snapshot_sync,
            bucket_id=bucket_id,
            include_gray=include_gray,
            max_expand_depth=max_expand_depth,
        )

    def _advance_collect_bucket_snapshot_sync(
        self,
        *,
        bucket_id: str | None,
        include_gray: bool,
        max_expand_depth: int | None,
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        eng = self.runtime.engine
        tree = eng.storage.topology_snapshot()
        resolved_bucket_id = self._advance_resolve_bucket_from_tree(bucket_id, tree)
        records_by_bucket = self._advance_collect_subtree_index_nodes(
            bucket_id=resolved_bucket_id,
            include_gray=include_gray,
            max_expand_depth=max_expand_depth,
            tree=tree,
        )
        root_node = self._advance_collect_bucket_tree_from_snapshot(
            bucket_id=resolved_bucket_id,
            include_gray=include_gray,
            max_expand_depth=max_expand_depth,
            depth=0,
            visited=set(),
            tree=tree,
            records_by_bucket=records_by_bucket,
        )
        return resolved_bucket_id, root_node, self._advance_render_top_payload(root_node)

    def _advance_collect_subtree_index_nodes(
        self,
        *,
        bucket_id: str,
        include_gray: bool,
        max_expand_depth: int | None,
        tree: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        """Read only compact locator nodes belonging to the requested subtree."""
        eng = self.runtime.engine
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            buckets = {}
        records_by_bucket: dict[str, list[dict[str, Any]]] = {}
        pending: list[tuple[str, int]] = [(bucket_id, 0)]
        visited: set[str] = set()
        while pending:
            current, depth = pending.pop()
            if current in visited or current not in buckets:
                continue
            visited.add(current)
            nodes = eng.storage.runtime_index_nodes_for_bucket(
                current,
                include_gray=include_gray,
            )
            records_by_bucket[current] = nodes
            if max_expand_depth is not None and depth >= max_expand_depth:
                continue
            child_ids = [
                str(node.get("child_bucket_id", "")).strip()
                for node in nodes
                if str(node.get("kind", "")) == BUCKET_KIND_BUCKET
            ]
            for child_id in reversed(child_ids):
                if child_id and child_id not in visited:
                    pending.append((child_id, depth + 1))
        return records_by_bucket

    @staticmethod
    def _advance_resolve_bucket_from_tree(bucket_id: str | None, tree: dict[str, Any]) -> str:
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            buckets = {}
        raw = str(bucket_id or "").strip()
        if raw.upper() == "ROOT":
            resolved = str(tree.get("root_bucket_id", "")).strip()
        else:
            active = str(tree.get("active_bucket_id", "")).strip()
            root = str(tree.get("root_bucket_id", "")).strip()
            resolved = raw or active or root

        visited: set[str] = {resolved}
        while True:
            current_raw = buckets.get(resolved)
            if not isinstance(current_raw, dict):
                raise ValueError(f"bucket not found: {resolved}")
            current = BucketInfo.from_dict(current_raw)
            next_id = str(current.sealed_to or "").strip() if current.sealed else ""
            if not next_id or next_id in visited or not isinstance(buckets.get(next_id), dict):
                return resolved
            resolved = next_id
            visited.add(resolved)

    @staticmethod
    def _advance_group_index_nodes_by_bucket(state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        keys = state.get("keys", {})
        if not isinstance(keys, dict):
            return grouped
        for node in keys.values():
            if not isinstance(node, dict):
                continue
            owner_bucket_id = str(node.get("bucket_id", "")).strip()
            if owner_bucket_id:
                grouped.setdefault(owner_bucket_id, []).append(node)
        return grouped

    def _advance_collect_bucket_tree_from_snapshot(
        self,
        *,
        bucket_id: str,
        include_gray: bool,
        max_expand_depth: int | None,
        depth: int,
        visited: set[str],
        tree: dict[str, Any],
        records_by_bucket: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        eng = self.runtime.engine
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            buckets = {}
        info_raw = buckets.get(bucket_id)
        if not isinstance(info_raw, dict):
            raise ValueError(f"bucket not found: {bucket_id}")
        info = BucketInfo.from_dict(info_raw)
        if bucket_id in visited:
            return {"bucket_id": bucket_id, "metadata": self._advance_bucket_metadata(info), "memories": [], "children": []}
        visited.add(bucket_id)
        try:
            index_nodes = records_by_bucket.get(bucket_id, [])
            memories: list[MemoryRecord] = []
            for index_node in index_nodes:
                if str(index_node.get("kind", BUCKET_KIND_MEMORY)) != BUCKET_KIND_MEMORY:
                    continue
                if not include_gray and bool(index_node.get("gray", False)):
                    continue
                rec = eng.storage.load_record_from_index_node(index_node)
                if rec is None or rec.kind != BUCKET_KIND_MEMORY or rec.bucket_id != bucket_id:
                    continue
                if not include_gray and rec.gray:
                    continue
                memories.append(rec)
            memories.sort(key=lambda r: str(r.key))
            memory_items = [
                {"key": rec.key, "metadata": self._advance_memory_metadata(rec), "content": rec.content}
                for rec in memories
            ]

            children: list[dict[str, Any]] = []
            if max_expand_depth is None or depth < max_expand_depth:
                child_ids: set[str] = set()
                for index_node in index_nodes:
                    if str(index_node.get("kind", "")) != BUCKET_KIND_BUCKET:
                        continue
                    if not include_gray and bool(index_node.get("gray", False)):
                        continue
                    child_id = str(index_node.get("child_bucket_id", "")).strip()
                    if child_id:
                        child_ids.add(child_id)
                child_infos: list[BucketInfo] = []
                for cid in child_ids:
                    child_raw = buckets.get(cid)
                    if isinstance(child_raw, dict):
                        child_infos.append(BucketInfo.from_dict(child_raw))
                child_infos.sort(key=lambda c: (float(c.last_event_at or 0.0), str(c.bucket_id)))
                for cinfo in child_infos:
                    children.append(
                        self._advance_collect_bucket_tree_from_snapshot(
                            bucket_id=cinfo.bucket_id,
                            include_gray=include_gray,
                            max_expand_depth=max_expand_depth,
                            depth=depth + 1,
                            visited=visited,
                            tree=tree,
                            records_by_bucket=records_by_bucket,
                        )
                    )

            return {
                "bucket_id": bucket_id,
                "metadata": self._advance_bucket_metadata(info),
                "memories": memory_items,
                "children": children,
            }
        finally:
            visited.discard(bucket_id)

    def _advance_render_bucket_node(self, node: dict[str, Any]) -> dict[str, Any]:
        content: dict[str, Any] = {}
        for mem in node.get("memories", []):
            mem_key = str(mem.get("key", "")).strip()
            if not mem_key:
                continue
            content[mem_key] = {
                "metadata": dict(mem.get("metadata", {})),
                "content": mem.get("content", ""),
            }
        for child in node.get("children", []):
            child_id = str(child.get("bucket_id", "")).strip()
            if not child_id:
                continue
            content[child_id] = self._advance_render_bucket_node(child)
        return {"metadata": dict(node.get("metadata", {})), "content": content}

    def _advance_render_top_payload(self, node: dict[str, Any]) -> dict[str, Any]:
        bucket_id = str(node.get("bucket_id", "")).strip()
        return {bucket_id: self._advance_render_bucket_node(node)}

    @staticmethod
    def _advance_build_user_markdown(*, command: str, payload: dict[str, Any]) -> str:
        user_command = str(command or "").strip()
        payload_text = json.dumps(payload, ensure_ascii=False, indent=2)
        return (
            "# 记忆库\n\n"
            f"{payload_text}\n\n"
            "---\n\n"
            "# 指令\n\n"
            f"{user_command}\n"
        )

    def _advance_build_full_markdown(self, *, system_text: str, command: str, payload: dict[str, Any]) -> str:
        user_markdown = self._advance_build_user_markdown(command=command, payload=payload)
        return self._advance_combine_request_markdown(system_text=system_text, user_markdown=user_markdown)

    @staticmethod
    def _advance_combine_request_markdown(*, system_text: str, user_markdown: str) -> str:
        return (
            "# System Prompt\n\n"
            f"{str(system_text or '').strip()}\n\n"
            "---\n\n"
            f"{user_markdown}"
        )

    async def _advance_prepare_components_for_llm(
        self,
        *,
        raw_payload: dict[str, Any],
        alias_bucket_id: str,
        enable_aliasing: bool,
        system_text: str,
        command: str,
    ) -> tuple[str, str, dict[str, Any]]:
        if not enable_aliasing:
            return str(system_text), str(command), raw_payload
        prepared = await self.runtime.engine.prepare_alias_payload(
            alias_bucket_id,
            {
                "system_text": str(system_text),
                "command": str(command),
                "payload": raw_payload,
            },
        )
        return (
            str(prepared.get("system_text", "")),
            str(prepared.get("command", "")),
            dict(prepared.get("payload", {})),
        )

    async def _advance_assert_request_safe(
        self,
        *,
        alias_bucket_id: str,
        enable_aliasing: bool,
        system_text: str,
        user_markdown: str,
    ) -> None:
        if enable_aliasing:
            await self.runtime.engine._assert_alias_payload_safe(
                alias_bucket_id,
                {"system_text": system_text, "user_markdown": user_markdown}
            )

    async def _advance_count_tokens_exact(self, text: str) -> int:
        return await asyncio.to_thread(self.runtime.engine.token_counter.count_text, text)

    async def _advance_prepare_payload_for_llm(
        self,
        *,
        raw_payload: dict[str, Any],
        alias_bucket_id: str,
        enable_aliasing: bool,
    ) -> dict[str, Any]:
        eng = self.runtime.engine
        if not enable_aliasing:
            return raw_payload
        return await eng.prepare_alias_payload(alias_bucket_id, raw_payload)

    async def _advance_payload_tokens(
        self,
        *,
        raw_payload: dict[str, Any],
        alias_bucket_id: str,
        enable_aliasing: bool,
        system_text: str,
        command: str,
    ) -> int:
        request_payload = await self._advance_prepare_payload_for_llm(
            raw_payload=raw_payload,
            alias_bucket_id=alias_bucket_id,
            enable_aliasing=enable_aliasing,
        )
        full_markdown = self._advance_build_full_markdown(
            system_text=system_text,
            command=command,
            payload=request_payload,
        )
        return await self._advance_count_tokens_exact(full_markdown)

    async def _advance_llm_request(
        self,
        *,
        system_text: str,
        user_markdown: str,
        llm_preset: str | None,
        tool_input: ToolInput | list[ToolInput] | Prompts | None,
        allow_tools: bool,
        alias_bucket_id: str | None = None,
    ) -> Prompts:
        eng = self.runtime.engine
        if alias_bucket_id:
            await eng._assert_alias_payload_safe(
                alias_bucket_id,
                {"system_text": system_text, "user_markdown": user_markdown}
            )
        preset = str(llm_preset or eng.llm_preset or "CONTEXT_MEMORY").strip()
        last_error: Exception | None = None
        for _ in range(2):
            chat = Chat(keep_alive=False)
            try:
                cfg = parse_llm_setting(preset)
                if cfg is None:
                    raise RuntimeError(f"advance_query preset not found: {preset}")
                endpoint = str(getattr(cfg, "endpoint", "") or "").strip()
                model = str(getattr(cfg, "model", "") or "").strip()
                if not endpoint or not model:
                    raise RuntimeError(f"advance_query preset invalid (missing endpoint/model): {preset}")
                chat.setting(cfg)
                if str(system_text or "").strip():
                    chat.add_context(SystemPrompt(str(system_text).strip()))
                if allow_tools and tool_input is not None:
                    chat.add_tools(tool_input)
                response = await chat.ask(TextPrompt("user", user_markdown), timeout=eng.pipeline.ask_timeout)
                if response is None:
                    raise RuntimeError("llm returned empty response")
                return response
            except Exception as exc:
                last_error = exc
            finally:
                try:
                    await chat.close()
                except Exception:
                    pass
        raise RuntimeError("advance_query request failed after one retry") from last_error

    @staticmethod
    def _advance_extract_text_response(resp: Prompts | Any) -> str:
        if isinstance(resp, Prompts):
            texts: list[str] = []
            for prompt in list(getattr(resp, "prompts", []) or []):
                if isinstance(prompt, TextPrompt) and str(getattr(prompt, "role", "")).lower() == "assistant":
                    text = str(getattr(prompt, "text", "") or "").strip()
                    if text:
                        texts.append(text)
            if texts:
                return "\n\n".join(texts)
        return ""

    @staticmethod
    def _advance_stable_serialize(value: Any) -> str:
        if isinstance(value, Prompts):
            return json.dumps(value.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
            try:
                return json.dumps(value.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            except Exception:
                pass
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return str(value)

    def _advance_chunk_response_content(self, *, label: str, response: Prompts | Any) -> str:
        text = self._advance_extract_text_response(response)
        if text:
            return f"[{label}]\n{text}"
        return self._advance_stable_serialize(response)

    def _advance_chunk_request_payload(
        self,
        *,
        bucket_id: str,
        bucket_metadata: dict[str, Any],
        label: str,
        source: list[str],
        content: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            bucket_id: {
                "metadata": dict(bucket_metadata),
                "content": {
                    label: {
                        "source": list(source),
                        "content": content,
                    }
                },
            }
        }

    def _advance_merge_box_contents(self, boxes: list[dict[str, Any]]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for box in boxes:
            content = box.get("content", {})
            if isinstance(content, dict):
                merged.update(content)
        return merged

    async def _advance_pack_boxes_first_fit(
        self,
        *,
        bucket_id: str,
        bucket_metadata: dict[str, Any],
        boxes: list[dict[str, Any]],
        label_prefix: str,
        threshold_tokens: int,
        system_text: str,
        command: str,
        alias_bucket_id: str,
        enable_aliasing: bool,
    ) -> list[dict[str, Any]]:
        chunks: list[list[dict[str, Any]]] = []
        for box in boxes:
            placed = False
            for chunk in chunks:
                candidate = list(chunk) + [box]
                probe_payload = self._advance_chunk_request_payload(
                    bucket_id=bucket_id,
                    bucket_metadata=bucket_metadata,
                    label=f"{bucket_id} {label_prefix} 1/1",
                    source=[str(x) for part in candidate for x in part.get("source", [])],
                    content=self._advance_merge_box_contents(candidate),
                )
                tok = await self._advance_payload_tokens(
                    raw_payload=probe_payload,
                    alias_bucket_id=alias_bucket_id,
                    enable_aliasing=enable_aliasing,
                    system_text=system_text,
                    command=command,
                )
                if tok <= threshold_tokens:
                    chunk.append(box)
                    placed = True
                    break
            if not placed:
                chunks.append([box])

        total = max(1, len(chunks))
        out: list[dict[str, Any]] = []
        for idx, chunk_boxes in enumerate(chunks, start=1):
            label = f"{bucket_id} {label_prefix} {idx}/{total}"
            out.append(
                {
                    "label": label,
                    "source": [str(x) for part in chunk_boxes for x in part.get("source", [])],
                    "content": self._advance_merge_box_contents(chunk_boxes),
                }
            )
        return out

    async def _advance_execute_chunks(
        self,
        *,
        chunk_specs: list[dict[str, Any]],
        bucket_id: str,
        bucket_metadata: dict[str, Any],
        command: str,
        system_text: str,
        llm_preset: str | None,
        alias_bucket_id: str,
        enable_aliasing: bool,
        parallel_limit: int,
        audit: bool,
    ) -> dict[str, dict[str, Any]]:
        sem = asyncio.Semaphore(max(1, int(parallel_limit)))
        results: list[tuple[int, dict[str, Any]]] = []

        async def _run_one(index: int, spec: dict[str, Any]) -> None:
            label = str(spec.get("label", "")).strip() or f"{bucket_id} chunk {index + 1}/?"
            payload = self._advance_chunk_request_payload(
                bucket_id=bucket_id,
                bucket_metadata=bucket_metadata,
                label=label,
                source=[str(x) for x in spec.get("source", [])],
                content=dict(spec.get("content", {})),
            )
            request_system, request_command, req_payload = await self._advance_prepare_components_for_llm(
                raw_payload=payload,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                system_text=system_text,
                command=command,
            )
            user_markdown = self._advance_build_user_markdown(command=request_command, payload=req_payload)
            await self._advance_assert_request_safe(
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                system_text=request_system,
                user_markdown=user_markdown,
            )
            async with sem:
                try:
                    resp = await self._advance_llm_request(
                        system_text=request_system,
                        user_markdown=user_markdown,
                        llm_preset=llm_preset,
                        tool_input=None,
                        allow_tools=False,
                        alias_bucket_id=alias_bucket_id if enable_aliasing else None,
                    )
                    content = self._advance_chunk_response_content(label=label, response=resp)
                    await self._advance_audit_event(
                        enabled=audit,
                        event_type="ADVANCE_QUERY_CHUNK_DONE",
                        bucket_id=bucket_id,
                        payload={"label": label, "source": spec.get("source", [])},
                    )
                except Exception as exc:
                    content = f"[{label}] [MISSING_AFTER_RETRY] {repr(exc)}"
                    await self._advance_audit_event(
                        enabled=audit,
                        event_type="ADVANCE_QUERY_CHUNK_FAIL",
                        bucket_id=bucket_id,
                        payload={"label": label, "source": spec.get("source", []), "error": repr(exc)},
                    )
                results.append(
                    (
                        index,
                        {
                            "label": label,
                            "item": {
                                "source": [str(x) for x in spec.get("source", [])],
                                "content": content,
                            },
                        },
                    )
                )

        await asyncio.gather(*[_run_one(i, s) for i, s in enumerate(chunk_specs)])
        results.sort(key=lambda x: x[0])
        ordered: dict[str, dict[str, Any]] = {}
        for _, row in results:
            ordered[str(row["label"])] = dict(row["item"])
        return ordered

    async def _advance_reduce_result_items(
        self,
        *,
        bucket_id: str,
        bucket_metadata: dict[str, Any],
        items: dict[str, dict[str, Any]],
        command: str,
        system_text: str,
        llm_preset: str | None,
        threshold_tokens: int,
        alias_bucket_id: str,
        enable_aliasing: bool,
        parallel_limit: int,
        audit: bool,
        allow_tools_on_final: bool,
        final_tool_input: ToolInput | list[ToolInput] | Prompts | None,
    ) -> Prompts:
        current_items: dict[str, dict[str, Any]] = dict(items)
        for _ in range(16):
            payload = {
                bucket_id: {
                    "metadata": dict(bucket_metadata),
                    "content": dict(current_items),
                }
            }
            tok = await self._advance_payload_tokens(
                raw_payload=payload,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                system_text=system_text,
                command=command,
            )
            if tok <= threshold_tokens:
                request_system, request_command, req_payload = await self._advance_prepare_components_for_llm(
                    raw_payload=payload,
                    alias_bucket_id=alias_bucket_id,
                    enable_aliasing=enable_aliasing,
                    system_text=system_text,
                    command=command,
                )
                user_markdown = self._advance_build_user_markdown(command=request_command, payload=req_payload)
                await self._advance_assert_request_safe(
                    alias_bucket_id=alias_bucket_id,
                    enable_aliasing=enable_aliasing,
                    system_text=request_system,
                    user_markdown=user_markdown,
                )
                return await self._advance_llm_request(
                    system_text=request_system,
                    user_markdown=user_markdown,
                    llm_preset=llm_preset,
                    tool_input=final_tool_input if allow_tools_on_final else None,
                    allow_tools=bool(allow_tools_on_final and final_tool_input is not None),
                    alias_bucket_id=alias_bucket_id if enable_aliasing else None,
                )

            result_boxes: list[dict[str, Any]] = []
            for label, item in current_items.items():
                result_boxes.append(
                    {
                        "source": [str(x) for x in item.get("source", [])],
                        "content": {
                            str(label): {
                                "source": [str(x) for x in item.get("source", [])],
                                "content": item.get("content", ""),
                            }
                        },
                    }
                )
            chunk_specs = await self._advance_pack_boxes_first_fit(
                bucket_id=bucket_id,
                bucket_metadata=bucket_metadata,
                boxes=result_boxes,
                label_prefix="result_chunk",
                threshold_tokens=threshold_tokens,
                system_text=system_text,
                command=command,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
            )
            current_items = await self._advance_execute_chunks(
                chunk_specs=chunk_specs,
                bucket_id=bucket_id,
                bucket_metadata=bucket_metadata,
                command=command,
                system_text=system_text,
                llm_preset=llm_preset,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                parallel_limit=parallel_limit,
                audit=audit,
            )
        raise RuntimeError("advance_query result_chunk exceeded reduction loop limit")

    async def _advance_run_best_effort_node(
        self,
        *,
        node: dict[str, Any],
        command: str,
        system_text: str,
        llm_preset: str | None,
        threshold_tokens: int,
        alias_bucket_id: str,
        enable_aliasing: bool,
        parallel_limit: int,
        audit: bool,
        allow_tools_on_final: bool,
        final_tool_input: ToolInput | list[ToolInput] | Prompts | None,
    ) -> Prompts:
        bucket_id = str(node.get("bucket_id", "")).strip()
        bucket_metadata = dict(node.get("metadata", {}))
        raw_payload = self._advance_render_top_payload(node)
        tok = await self._advance_payload_tokens(
            raw_payload=raw_payload,
            alias_bucket_id=alias_bucket_id,
            enable_aliasing=enable_aliasing,
            system_text=system_text,
            command=command,
        )
        if tok <= threshold_tokens:
            request_system, request_command, req_payload = await self._advance_prepare_components_for_llm(
                raw_payload=raw_payload,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                system_text=system_text,
                command=command,
            )
            user_markdown = self._advance_build_user_markdown(command=request_command, payload=req_payload)
            await self._advance_assert_request_safe(
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                system_text=request_system,
                user_markdown=user_markdown,
            )
            return await self._advance_llm_request(
                system_text=request_system,
                user_markdown=user_markdown,
                llm_preset=llm_preset,
                tool_input=final_tool_input if allow_tools_on_final else None,
                allow_tools=bool(allow_tools_on_final and final_tool_input is not None),
                alias_bucket_id=alias_bucket_id if enable_aliasing else None,
            )

        memory_content: dict[str, Any] = {}
        for mem in node.get("memories", []):
            mem_key = str(mem.get("key", "")).strip()
            if not mem_key:
                continue
            memory_content[mem_key] = {"metadata": dict(mem.get("metadata", {})), "content": mem.get("content", "")}

        boxes: list[dict[str, Any]] = []
        if memory_content:
            mem_box = {"source": [f"{bucket_id}_memories"], "content": dict(memory_content), "kind": "memory"}
            mem_probe = self._advance_chunk_request_payload(
                bucket_id=bucket_id,
                bucket_metadata=bucket_metadata,
                label=f"{bucket_id} chunk 1/1",
                source=list(mem_box["source"]),
                content=dict(mem_box["content"]),
            )
            mem_tok = await self._advance_payload_tokens(
                raw_payload=mem_probe,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                system_text=system_text,
                command=command,
            )
            if mem_tok > threshold_tokens:
                raise RuntimeError(
                    f"advance_query memory box overflow in bucket={bucket_id}; "
                    "please run compress/split first."
                )
            boxes.append(mem_box)

        for child in node.get("children", []):
            child_id = str(child.get("bucket_id", "")).strip()
            if not child_id:
                continue
            child_box_content = {child_id: self._advance_render_bucket_node(child)}
            child_box = {"source": [child_id], "content": child_box_content, "kind": "child"}
            child_probe = self._advance_chunk_request_payload(
                bucket_id=bucket_id,
                bucket_metadata=bucket_metadata,
                label=f"{bucket_id} chunk 1/1",
                source=list(child_box["source"]),
                content=dict(child_box["content"]),
            )
            child_tok = await self._advance_payload_tokens(
                raw_payload=child_probe,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                system_text=system_text,
                command=command,
            )
            if child_tok > threshold_tokens:
                child_resp = await self._advance_run_best_effort_node(
                    node=child,
                    command=command,
                    system_text=system_text,
                    llm_preset=llm_preset,
                    threshold_tokens=threshold_tokens,
                    alias_bucket_id=alias_bucket_id,
                    enable_aliasing=enable_aliasing,
                    parallel_limit=parallel_limit,
                    audit=audit,
                    allow_tools_on_final=False,
                    final_tool_input=None,
                )
                child_box = {
                    "source": [child_id],
                    "content": {child_id: self._advance_chunk_response_content(label=f"{child_id} chunk 1/1", response=child_resp)},
                    "kind": "child",
                }
                child_probe = self._advance_chunk_request_payload(
                    bucket_id=bucket_id,
                    bucket_metadata=bucket_metadata,
                    label=f"{bucket_id} chunk 1/1",
                    source=list(child_box["source"]),
                    content=dict(child_box["content"]),
                )
                child_tok = await self._advance_payload_tokens(
                    raw_payload=child_probe,
                    alias_bucket_id=alias_bucket_id,
                    enable_aliasing=enable_aliasing,
                    system_text=system_text,
                    command=command,
                )
                if child_tok > threshold_tokens:
                    raise RuntimeError(
                        f"advance_query child box overflow unresolved in bucket={bucket_id}, child={child_id}"
                    )
            boxes.append(child_box)

        chunk_specs = await self._advance_pack_boxes_first_fit(
            bucket_id=bucket_id,
            bucket_metadata=bucket_metadata,
            boxes=boxes,
            label_prefix="chunk",
            threshold_tokens=threshold_tokens,
            system_text=system_text,
            command=command,
            alias_bucket_id=alias_bucket_id,
            enable_aliasing=enable_aliasing,
        )
        chunk_items = await self._advance_execute_chunks(
            chunk_specs=chunk_specs,
            bucket_id=bucket_id,
            bucket_metadata=bucket_metadata,
            command=command,
            system_text=system_text,
            llm_preset=llm_preset,
            alias_bucket_id=alias_bucket_id,
            enable_aliasing=enable_aliasing,
            parallel_limit=parallel_limit,
            audit=audit,
        )
        return await self._advance_reduce_result_items(
            bucket_id=bucket_id,
            bucket_metadata=bucket_metadata,
            items=chunk_items,
            command=command,
            system_text=system_text,
            llm_preset=llm_preset,
            threshold_tokens=threshold_tokens,
            alias_bucket_id=alias_bucket_id,
            enable_aliasing=enable_aliasing,
            parallel_limit=parallel_limit,
            audit=audit,
            allow_tools_on_final=allow_tools_on_final,
            final_tool_input=final_tool_input,
        )

    async def _advance_audit_event(
        self,
        *,
        enabled: bool,
        event_type: str,
        bucket_id: str,
        payload: dict[str, Any],
    ) -> None:
        eng = self.runtime.engine
        if not enabled:
            return
        try:
            await eng._run_storage_task(
                eng.storage.append_event,
                event_type=event_type,
                bucket_id=bucket_id,
                payload=dict(payload),
            )
        except Exception:
            pass
