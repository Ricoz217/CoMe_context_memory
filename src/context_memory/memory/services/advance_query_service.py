from __future__ import annotations

import asyncio
import json
from typing import Any, Literal

from context_memory.LLM_connect import Chat, Prompts, SystemPrompt, TextPrompt, ToolInput, parse_llm_setting

from ..models import BUCKET_KIND_BUCKET, BUCKET_KIND_MEMORY, BucketInfo, MemoryRecord
from .runtime import ServiceRuntime

try:
    import tiktoken  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    tiktoken = None  # type: ignore


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
    ) -> Any:
        eng = self.runtime.engine
        mode_value = self._normalize_advance_query_mode(mode)
        target_bucket_id = eng._resolve_bucket_id(bucket_id)
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
            root_node = self._advance_collect_bucket_tree(
                bucket_id=target_bucket_id,
                include_gray=bool(include_gray),
                max_expand_depth=(None if max_expand_depth is None else int(max_expand_depth)),
                depth=0,
                visited=set(),
            )
            full_payload = self._advance_render_top_payload(root_node)
            token_count = self._advance_payload_tokens(
                raw_payload=full_payload,
                alias_bucket_id=target_bucket_id,
                enable_aliasing=bool(enable_aliasing),
                system_text=system_text,
                command=command,
            )
            self._advance_audit_event(
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
                user_markdown = self._advance_build_user_markdown(command=command, payload=full_payload)
                return await self._advance_llm_request(
                    system_text=system_text,
                    user_markdown=user_markdown,
                    llm_preset=llm_preset,
                    tool_input=tool_input,
                    allow_tools=True,
                )

            response = await self._advance_run_best_effort_node(
                node=root_node,
                command=command,
                system_text=system_text,
                llm_preset=llm_preset,
                threshold_tokens=threshold_tokens,
                alias_bucket_id=target_bucket_id,
                enable_aliasing=bool(enable_aliasing),
                parallel_limit=parallel_limit,
                audit=bool(audit),
                allow_tools_on_final=True,
                final_tool_input=tool_input,
            )
            self._advance_audit_event(
                enabled=bool(audit),
                event_type="ADVANCE_QUERY_SUCCESS",
                bucket_id=target_bucket_id,
                payload={"mode": mode_value, "threshold_tokens": threshold_tokens},
            )
            return response
        except Exception as exc:
            self._advance_audit_event(
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
        eng = self.runtime.engine
        info = eng.storage.get_bucket_info(bucket_id)
        if info is None:
            raise ValueError(f"bucket not found: {bucket_id}")
        if bucket_id in visited:
            return {"bucket_id": bucket_id, "metadata": self._advance_bucket_metadata(info), "memories": [], "children": []}
        visited.add(bucket_id)
        try:
            records = eng.storage.list_bucket_records(bucket_id, include_gray=include_gray)
            memories = [x for x in records if x.kind == BUCKET_KIND_MEMORY]
            memories.sort(key=lambda r: str(r.key))
            memory_items = [
                {"key": rec.key, "metadata": self._advance_memory_metadata(rec), "content": rec.content}
                for rec in memories
            ]

            children: list[dict[str, Any]] = []
            if max_expand_depth is None or depth < max_expand_depth:
                child_ids: set[str] = set()
                for rec in records:
                    if rec.kind == BUCKET_KIND_BUCKET and str(rec.child_bucket_id or "").strip():
                        child_ids.add(str(rec.child_bucket_id).strip())
                child_infos: list[BucketInfo] = []
                for cid in child_ids:
                    cinfo = eng.storage.get_bucket_info(cid)
                    if cinfo is not None:
                        child_infos.append(cinfo)
                child_infos.sort(key=lambda c: (float(c.last_event_at or 0.0), str(c.bucket_id)))
                for cinfo in child_infos:
                    children.append(
                        self._advance_collect_bucket_tree(
                            bucket_id=cinfo.bucket_id,
                            include_gray=include_gray,
                            max_expand_depth=max_expand_depth,
                            depth=depth + 1,
                            visited=visited,
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
            "# User Prompt (Payload)\n\n"
            f"{user_command}\n\n"
            "---\n\n"
            "# 璁板繂搴揬n"
            f"{payload_text}\n"
        )

    def _advance_build_full_markdown(self, *, system_text: str, command: str, payload: dict[str, Any]) -> str:
        user_markdown = self._advance_build_user_markdown(command=command, payload=payload)
        return (
            "# System Prompt\n\n"
            f"{str(system_text or '').strip()}\n\n"
            "---\n\n"
            f"{user_markdown}"
        )

    def _advance_count_tokens_exact(self, text: str) -> int:
        if tiktoken is None:
            raise RuntimeError("advance_query requires tiktoken for exact token estimation")
        try:
            enc = tiktoken.get_encoding("o200k_base")
            return max(1, int(len(enc.encode(str(text)))))
        except Exception as exc:
            raise RuntimeError("advance_query failed to estimate tokens via tiktoken") from exc

    def _advance_prepare_payload_for_llm(
        self,
        *,
        raw_payload: dict[str, Any],
        alias_bucket_id: str,
        enable_aliasing: bool,
    ) -> dict[str, Any]:
        eng = self.runtime.engine
        if not enable_aliasing:
            return raw_payload
        alias_payload = eng.build_llm_view(alias_bucket_id, raw_payload)
        eng.assert_alias_only_payload(alias_bucket_id, alias_payload)
        return alias_payload

    def _advance_payload_tokens(
        self,
        *,
        raw_payload: dict[str, Any],
        alias_bucket_id: str,
        enable_aliasing: bool,
        system_text: str,
        command: str,
    ) -> int:
        request_payload = self._advance_prepare_payload_for_llm(
            raw_payload=raw_payload,
            alias_bucket_id=alias_bucket_id,
            enable_aliasing=enable_aliasing,
        )
        full_markdown = self._advance_build_full_markdown(
            system_text=system_text,
            command=command,
            payload=request_payload,
        )
        return self._advance_count_tokens_exact(full_markdown)

    async def _advance_llm_request(
        self,
        *,
        system_text: str,
        user_markdown: str,
        llm_preset: str | None,
        tool_input: ToolInput | list[ToolInput] | Prompts | None,
        allow_tools: bool,
    ) -> Prompts:
        eng = self.runtime.engine
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

    def _advance_pack_boxes_first_fit(
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
                tok = self._advance_payload_tokens(
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
            req_payload = self._advance_prepare_payload_for_llm(
                raw_payload=payload,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
            )
            user_markdown = self._advance_build_user_markdown(command=command, payload=req_payload)
            async with sem:
                try:
                    resp = await self._advance_llm_request(
                        system_text=system_text,
                        user_markdown=user_markdown,
                        llm_preset=llm_preset,
                        tool_input=None,
                        allow_tools=False,
                    )
                    content = self._advance_chunk_response_content(label=label, response=resp)
                    self._advance_audit_event(
                        enabled=audit,
                        event_type="ADVANCE_QUERY_CHUNK_DONE",
                        bucket_id=bucket_id,
                        payload={"label": label, "source": spec.get("source", [])},
                    )
                except Exception as exc:
                    content = f"[{label}] [MISSING_AFTER_RETRY] {repr(exc)}"
                    self._advance_audit_event(
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
            tok = self._advance_payload_tokens(
                raw_payload=payload,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
                system_text=system_text,
                command=command,
            )
            if tok <= threshold_tokens:
                req_payload = self._advance_prepare_payload_for_llm(
                    raw_payload=payload,
                    alias_bucket_id=alias_bucket_id,
                    enable_aliasing=enable_aliasing,
                )
                user_markdown = self._advance_build_user_markdown(command=command, payload=req_payload)
                return await self._advance_llm_request(
                    system_text=system_text,
                    user_markdown=user_markdown,
                    llm_preset=llm_preset,
                    tool_input=final_tool_input if allow_tools_on_final else None,
                    allow_tools=bool(allow_tools_on_final and final_tool_input is not None),
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
            chunk_specs = self._advance_pack_boxes_first_fit(
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
        tok = self._advance_payload_tokens(
            raw_payload=raw_payload,
            alias_bucket_id=alias_bucket_id,
            enable_aliasing=enable_aliasing,
            system_text=system_text,
            command=command,
        )
        if tok <= threshold_tokens:
            req_payload = self._advance_prepare_payload_for_llm(
                raw_payload=raw_payload,
                alias_bucket_id=alias_bucket_id,
                enable_aliasing=enable_aliasing,
            )
            user_markdown = self._advance_build_user_markdown(command=command, payload=req_payload)
            return await self._advance_llm_request(
                system_text=system_text,
                user_markdown=user_markdown,
                llm_preset=llm_preset,
                tool_input=final_tool_input if allow_tools_on_final else None,
                allow_tools=bool(allow_tools_on_final and final_tool_input is not None),
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
            mem_tok = self._advance_payload_tokens(
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
            child_tok = self._advance_payload_tokens(
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
                child_tok = self._advance_payload_tokens(
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

        chunk_specs = self._advance_pack_boxes_first_fit(
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

    def _advance_audit_event(
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
            eng.storage.append_event(
                event_type=event_type,
                bucket_id=bucket_id,
                payload=dict(payload),
            )
        except Exception:
            pass

