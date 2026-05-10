from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any
from typing import TYPE_CHECKING

from context_memory.LLM_connect import Chat, ChatConfig, SystemPrompt, TextPrompt, parse_llm_setting

from .models import normalize_relations

if TYPE_CHECKING:
    from context_memory.LLM_usage import LLMUsage
    from context_memory.utils import AutoMapping


class LLMPresetConfigError(RuntimeError):
    pass


class LLMPipelineV3:
    TOOL_PRESET_KEYS = {
        "clean",
        "ingest",
        "query",
        "compress",
        "bucket_split",
        "text_chunk",
        "bucket_summary",
        "optimize",
    }

    def __init__(
        self,
        prompt_dir: str | Path,
        *,
        llm_preset: str = "CONTEXT_MEMORY",
        tool_presets: dict[str, str] | None = None,
        ask_timeout: float = 180.0,
        max_retries: int = 2,
        use_mock_llm: bool = False,
        enable_cleaning: bool = True,
        init_config: bool = True,
        usage_store: "LLMUsage | None" = None,
        image_name_mapping: "AutoMapping[list[str]] | None" = None,
    ) -> None:
        self.prompt_dir = Path(prompt_dir)
        self.llm_preset = llm_preset
        self.default_llm_preset = llm_preset
        self.tool_presets = self._normalize_tool_presets(tool_presets)
        self.ask_timeout = ask_timeout
        self.max_retries = max(1, int(max_retries))
        self.use_mock_llm = use_mock_llm
        self.enable_cleaning = enable_cleaning
        self.init_config = init_config
        self.usage_store = usage_store
        self.image_name_mapping = image_name_mapping
        self._prompt_cache: dict[str, str] = {}
        self._last_usage: dict[str, int] = {
            "calls": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_input_tokens": 0,
        }
        self._last_diag: dict[str, Any] = {
            "degraded": False,
            "degraded_reason": "",
            "parse_failed": False,
            "precheck_failed": False,
            "failure_stage": "",
        }

    @property
    def last_usage(self) -> dict[str, int]:
        return dict(self._last_usage)

    @property
    def last_diagnostics(self) -> dict[str, Any]:
        return dict(self._last_diag)

    async def clean(self, *, raw_text: str, evidence_text: str = "") -> dict[str, Any]:
        if not self.enable_cleaning:
            source = str(raw_text or "")
            is_source_code = self._looks_like_source_code(source)
            return self._accept_clean_result(
                source,
                input_type="source_code" if is_source_code else "plain",
                skip_clean=is_source_code,
                preserve_literal=is_source_code,
            )

        payload = {
            "raw_text": raw_text,
            "evidence_text": evidence_text,
        }
        if self.use_mock_llm:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(degraded=True, degraded_reason="mock_clean", failure_stage="")
            return self._fallback_clean(payload)

        result = await self._ask_json(
            prompt_filename="clean_system.md",
            schema_filename="clean_schema.md",
            user_payload=payload,
            bucket_context=None,
            include_context=False,
            preset_key="clean",
        )
        if not result:
            self._set_diag(
                degraded=True,
                degraded_reason=self._fallback_reason("clean_fallback"),
                failure_stage=self._last_diag.get("failure_stage", ""),
            )
            return self._fallback_clean(payload)

        normalized = self._normalize_clean_result(result, payload)
        if (
            not bool(normalized.get("skip_clean", False))
            and not normalized.get("clean_text", "")
            and bool(normalized.get("accept", True))
        ):
            return self._fallback_clean(payload)
        self._set_diag(degraded=False, degraded_reason="", failure_stage="")
        return normalized

    async def ingest(
        self,
        *,
        bucket_context: Any,
        key: str,
        event: str,
        raw_text: str,
        evidence_text: str = "",
        topic: str = "",
        input_type: str = "",
        skip_clean: bool = False,
        preserve_literal: bool = False,
        previous_record: dict[str, Any] | None = None,
        split_chunks: list[dict[str, Any]] | None = None,
        split_keys: list[str] | None = None,
        split_index: int | None = None,
        split_total: int | None = None,
        default_weight: float | None = None,
    ) -> dict[str, Any]:
        # Keep stable split context at the front and put per-chunk dynamic fields later
        # so prompt-cache prefix can reuse more tokens across chunk-ingest calls.
        payload = {
            "event": event,
            "input_type": input_type,
            "skip_clean": skip_clean,
            "preserve_literal": preserve_literal,
            "split_total": int(split_total) if split_total is not None else None,
            "split_chunks": split_chunks or [],
            "split_keys": split_keys or [],
            "default_weight": default_weight,
            "evidence_text": evidence_text,
            "previous_record": previous_record or {},
            "topic": topic,
            "key": key,
            "split_index": int(split_index) if split_index is not None else None,
            "raw_text": raw_text,
        }
        if self.use_mock_llm:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(degraded=True, degraded_reason="mock_ingest", failure_stage="")
            return self._fallback_ingest(payload)

        result = await self._ask_json(
            prompt_filename="ingest_system.md",
            schema_filename="ingest_schema.md",
            user_payload=payload,
            bucket_context=bucket_context,
            include_context=True,
            sort_keys=False,
            preset_key="ingest",
        )
        if not result:
            self._set_diag(
                degraded=True,
                degraded_reason=self._fallback_reason("ingest_fallback"),
                failure_stage=self._last_diag.get("failure_stage", ""),
            )
            return self._fallback_ingest(payload)
        self._set_diag(degraded=False, degraded_reason="", failure_stage="")
        return self._normalize_ingest_result(result, payload)

    async def query(
        self,
        *,
        bucket_context: Any,
        query_text: str,
        top_k: int,
        include_gray: bool,
        key_hints: list[str],
        fallback_candidates: list[tuple[dict[str, Any], float]],
    ) -> dict[str, Any]:
        payload = {
            "query_text": query_text,
            "top_k": top_k,
            "include_gray": include_gray,
            "key_hints": key_hints,
            "hint_count": len(key_hints),
        }
        if self.use_mock_llm:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(degraded=True, degraded_reason="mock_query", failure_stage="")
            return self._fallback_query(payload, fallback_candidates)

        result = await self._ask_json(
            prompt_filename="query_system.md",
            schema_filename="query_schema.md",
            user_payload=payload,
            bucket_context=bucket_context,
            include_context=True,
            preset_key="query",
        )
        if not result:
            self._set_diag(
                degraded=True,
                degraded_reason=self._fallback_reason("query_fallback"),
                failure_stage=self._last_diag.get("failure_stage", ""),
            )
            return self._fallback_query(payload, fallback_candidates)

        normalized = self._normalize_query_result(result, payload)
        if not normalized.get("matches"):
            self._set_diag(degraded=True, degraded_reason="query_empty_matches", failure_stage="")
            return self._fallback_query(payload, fallback_candidates)
        self._set_diag(degraded=False, degraded_reason="", failure_stage="")
        return normalized

    async def compress(
        self,
        *,
        bucket_context: Any,
        records: list[dict[str, Any]],
        reason: str,
        estimated_tokens: int,
        max_estimated_tokens: int,
    ) -> dict[str, Any]:
        payload = {
            "reason": reason,
            "estimated_tokens": estimated_tokens,
            "max_estimated_tokens": max_estimated_tokens,
            "records": records,
        }
        if self.use_mock_llm:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(degraded=True, degraded_reason="mock_compress", failure_stage="")
            return self._fallback_compress(payload)

        result = await self._ask_json(
            prompt_filename="compress_system.md",
            schema_filename="compress_schema.md",
            user_payload=payload,
            bucket_context=bucket_context,
            include_context=True,
            preset_key="compress",
        )
        if not result:
            self._set_diag(
                degraded=True,
                degraded_reason=self._fallback_reason("compress_fallback"),
                failure_stage=self._last_diag.get("failure_stage", ""),
            )
            return self._fallback_compress(payload)
        self._set_diag(degraded=False, degraded_reason="", failure_stage="")
        return self._normalize_compress_result(result, payload)

    async def bucket_split(
        self,
        *,
        bucket_context: Any,
        records: list[dict[str, Any]],
        split_plan_target_items: int = 180,
        split_plan_hard_cap: int = 250,
        target_groups_min: int = 2,
        target_groups_max: int = 10,
        reason: str = "auto_split",
    ) -> dict[str, Any]:
        payload = {
            "reason": reason,
            "split_plan_target_items": max(1, int(split_plan_target_items)),
            "split_plan_hard_cap": max(1, int(split_plan_hard_cap)),
            "target_groups_min": max(2, int(target_groups_min)),
            "target_groups_max": max(2, int(target_groups_max)),
            "records": records,
        }
        if self.use_mock_llm:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(degraded=True, degraded_reason="mock_bucket_split", failure_stage="")
            return self._fallback_split(payload)

        result = await self._ask_json(
            prompt_filename="bucket_split_system.md",
            schema_filename="bucket_split_schema.md",
            user_payload=payload,
            bucket_context=bucket_context,
            include_context=True,
            preset_key="bucket_split",
        )
        if not result:
            self._set_diag(
                degraded=True,
                degraded_reason=self._fallback_reason("bucket_split_fallback"),
                failure_stage=self._last_diag.get("failure_stage", ""),
            )
            return self._fallback_split(payload)
        self._set_diag(degraded=False, degraded_reason="", failure_stage="")
        return self._normalize_split_result(result, payload)

    async def split_bucket(
        self,
        *,
        bucket_context: Any,
        records: list[dict[str, Any]],
        split_plan_target_items: int = 180,
        split_plan_hard_cap: int = 250,
        target_groups_min: int = 2,
        target_groups_max: int = 10,
        reason: str = "auto_split",
    ) -> dict[str, Any]:
        # Backward-compatible alias.
        return await self.bucket_split(
            bucket_context=bucket_context,
            records=records,
            split_plan_target_items=split_plan_target_items,
            split_plan_hard_cap=split_plan_hard_cap,
            target_groups_min=target_groups_min,
            target_groups_max=target_groups_max,
            reason=reason,
        )

    async def text_chunk(
        self,
        *,
        raw_text: str,
        topic: str = "",
        chunk_max_chars: int = 4000,
        chunk_overlap_chars: int = 200,
        reason: str = "force_split",
    ) -> dict[str, Any]:
        line_list = self._split_lines(str(raw_text or ""))
        line_map = {str(i + 1): line for i, line in enumerate(line_list)}
        payload = {
            "raw_text": raw_text,
            "topic": topic,
            "chunk_max_chars": max(100, int(chunk_max_chars)),
            "chunk_overlap_chars": max(0, int(chunk_overlap_chars)),
            "reason": reason,
            "line_list": line_list,
        }
        if payload["chunk_overlap_chars"] >= payload["chunk_max_chars"]:
            payload["chunk_overlap_chars"] = payload["chunk_max_chars"] // 4

        if self.use_mock_llm:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(degraded=True, degraded_reason="mock_text_chunk", failure_stage="")
            return self._fallback_text_chunk(payload)

        llm_payload = {
            "topic": topic,
            "reason": reason,
            "chunk_max_chars": payload["chunk_max_chars"],
            "chunk_overlap_chars": payload["chunk_overlap_chars"],
            "line_count": len(line_list),
            "line_map": line_map,
        }
        result = await self._ask_json(
            prompt_filename="text_chunk_line_system.md",
            schema_filename="text_chunk_line_schema.md",
            user_payload=llm_payload,
            bucket_context=None,
            include_context=False,
            preset_key="text_chunk",
        )
        if not result:
            self._set_diag(
                degraded=True,
                degraded_reason=self._fallback_reason("text_chunk_fallback"),
                failure_stage=self._last_diag.get("failure_stage", ""),
            )
            return self._fallback_text_chunk(payload)
        normalized = self._normalize_text_chunk_result(result, payload)
        if not normalized.get("chunks"):
            self._set_diag(
                degraded=True,
                degraded_reason="text_chunk_empty",
                failure_stage="",
            )
            return self._fallback_text_chunk(payload)
        self._set_diag(degraded=False, degraded_reason="", failure_stage="")
        return normalized

    async def summarize_bucket(self, *, records: list[dict[str, Any]], reason: str = "bucket_summary") -> dict[str, str]:
        payload = {"reason": reason, "records": records}
        if self.use_mock_llm:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(degraded=True, degraded_reason="mock_bucket_summary", failure_stage="")
            return self._fallback_bucket_summary(payload)

        result = await self._ask_json(
            prompt_filename="bucket_summary_system.md",
            schema_filename="bucket_summary_schema.md",
            user_payload=payload,
            bucket_context=None,
            include_context=False,
            preset_key="bucket_summary",
        )
        if not result:
            self._set_diag(
                degraded=True,
                degraded_reason=self._fallback_reason("bucket_summary_fallback"),
                failure_stage=self._last_diag.get("failure_stage", ""),
            )
            return self._fallback_bucket_summary(payload)
        self._set_diag(degraded=False, degraded_reason="", failure_stage="")
        return self._normalize_bucket_summary(result)

    async def optimize(
        self,
        *,
        bucket_context: Any,
        reason: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        request_payload = {
            "reason": reason,
            "payload": payload,
        }
        if self.use_mock_llm:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(degraded=True, degraded_reason="mock_optimize", failure_stage="")
            return self._fallback_optimize(request_payload)

        result = await self._ask_json(
            prompt_filename="optimize_system.md",
            schema_filename="optimize_schema.md",
            user_payload=request_payload,
            bucket_context=bucket_context,
            include_context=False,
            preset_key="optimize",
        )
        if not result:
            self._set_diag(
                degraded=True,
                degraded_reason=self._fallback_reason("optimize_fallback"),
                failure_stage=self._last_diag.get("failure_stage", ""),
            )
            return self._fallback_optimize(request_payload)
        self._set_diag(degraded=False, degraded_reason="", failure_stage="")
        return self._normalize_optimize_result(result)

    async def _ask_json(
        self,
        *,
        prompt_filename: str,
        schema_filename: str,
        user_payload: dict[str, Any],
        bucket_context: Any,
        include_context: bool,
        sort_keys: bool = True,
        preset_key: str | None = None,
    ) -> dict[str, Any] | None:
        self._set_diag(degraded=False, degraded_reason="", parse_failed=False, precheck_failed=False, failure_stage="")

        config = self._load_llm_config(preset_key=preset_key)
        if config is None:
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(precheck_failed=True, failure_stage="precheck")
            return None

        schema_text = self._load_prompt(schema_filename)
        system_text = self._load_prompt(prompt_filename)
        if not schema_text.strip() or not system_text.strip():
            self._set_last_usage(calls=0, input_tokens=0, output_tokens=0, cached_input_tokens=0)
            self._set_diag(precheck_failed=True, failure_stage="prompt_missing")
            return None
        full_system = f"{system_text}\n\n{schema_text}".strip()
        request_json = json.dumps(
            user_payload,
            ensure_ascii=False,
            sort_keys=bool(sort_keys),
            separators=(",", ":"),
        )

        usage_acc = {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cached_input_tokens": 0}

        for _ in range(self.max_retries):
            chat = Chat(
                keep_alive=False,
                usage_store=self.usage_store,
                image_name_mapping=self.image_name_mapping,
            )
            chat.setting(config)
            try:
                forced_params = dict(getattr(config, "model_params", {}) or {})
                rf = forced_params.get("response_format", {})
                if not isinstance(rf, dict):
                    rf = {}
                rf.update({"type": "json_object"})
                forced_params["response_format"] = rf
                # Force non-stream for stability and simpler JSON parsing in preview v2.
                if ChatConfig is not None:
                    chat.setting(ChatConfig(model_params=forced_params, keep_alive=False))

                chat.add_context(SystemPrompt(full_system))
                if include_context and bucket_context is not None:
                    try:
                        safe_context = bucket_context.copy() if hasattr(bucket_context, "copy") else bucket_context
                        prompts = safe_context.to_prompts()
                        if getattr(prompts, "prompts", None):
                            chat.add_context(prompts)
                    except Exception:
                        pass

                response = await chat.ask(TextPrompt("user", request_json), timeout=self.ask_timeout)
                usage = self._extract_usage(response)
                usage_acc["calls"] += usage["calls"]
                usage_acc["input_tokens"] += usage["input_tokens"]
                usage_acc["output_tokens"] += usage["output_tokens"]
                usage_acc["cached_input_tokens"] += usage["cached_input_tokens"]
                text = self._extract_text_response(response)
                parsed = self._parse_json_text(text)
                if parsed is not None:
                    self._set_last_usage(**usage_acc)
                    return parsed
            except Exception as exc:
                exc_type = type(exc).__name__
                exc_msg = str(exc)
                if self._is_context_overflow_error(exc_type=exc_type, exc_msg=exc_msg):
                    self._set_diag(failure_stage="context_overflow")
                    break
            finally:
                try:
                    await chat.close()
                except Exception:
                    pass

        self._set_last_usage(**usage_acc)
        if str(self._last_diag.get("failure_stage", "")) == "context_overflow":
            self._set_diag(parse_failed=False)
        else:
            self._set_diag(parse_failed=True, failure_stage="parse")
        return None

    def _fallback_reason(self, default_reason: str) -> str:
        stage = str(self._last_diag.get("failure_stage", "")).strip().lower()
        if stage == "context_overflow":
            return "context_overflow"
        return default_reason

    @staticmethod
    def _is_context_overflow_error(*, exc_type: str, exc_msg: str) -> bool:
        et = str(exc_type or "").lower()
        em = str(exc_msg or "").lower()
        if "contextoverflowerror" in et:
            return True
        hints = (
            "context overflow",
            "context too long",
            "maximum context length",
            "prompt is too long",
            "token limit",
            "超窗",
            "上下文过长",
        )
        return any(h in em for h in hints)

    def _load_llm_config(self, *, preset_key: str | None = None):
        preset_name = self._resolve_preset_name(preset_key)
        if not str(preset_name).strip():
            raise LLMPresetConfigError(
                f"LLM preset is empty for key={preset_key or 'default'}; "
                "please set llm_preset or tool_presets."
            )
        try:
            config = parse_llm_setting(preset_name)
        except Exception as exc:
            raise LLMPresetConfigError(
                f"parse_llm_setting failed for preset '{preset_name}': {exc}"
            ) from exc
        if config is None:
            return None
        endpoint = str(getattr(config, "endpoint", "") or "").strip()
        model = str(getattr(config, "model", "") or "").strip()
        if not endpoint or not model:
            return None
        return config

    def _resolve_preset_name(self, preset_key: str | None) -> str:
        if not preset_key:
            return self.default_llm_preset
        key = str(preset_key).strip().lower()
        preset = self.tool_presets.get(key, "")
        if isinstance(preset, str) and preset.strip():
            return preset.strip()
        return self.default_llm_preset

    def _normalize_tool_presets(self, tool_presets: dict[str, str] | None) -> dict[str, str]:
        if not isinstance(tool_presets, dict):
            return {}
        normalized: dict[str, str] = {}
        for k, v in tool_presets.items():
            key = str(k).strip().lower()
            val = str(v).strip()
            if key in self.TOOL_PRESET_KEYS and val:
                normalized[key] = val
        return normalized

    def _extract_text_response(self, response: Any) -> str:
        if response is None:
            return ""
        texts: list[str] = []
        prompts = getattr(response, "prompts", [])
        for p in prompts:
            role = getattr(p, "role", "")
            text = getattr(p, "text", "")
            if role == "assistant" and isinstance(text, str):
                texts.append(text)
        return "\n".join(texts).strip()

    @staticmethod
    def _extract_usage(response: Any) -> dict[str, int]:
        usage_obj = getattr(response, "usage", None)
        if usage_obj is None:
            return {
                "calls": 1 if response is not None else 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cached_input_tokens": 0,
            }

        try:
            input_tokens = int(getattr(usage_obj, "input_t", 0) or 0)
        except (TypeError, ValueError):
            input_tokens = 0
        try:
            output_tokens = int(getattr(usage_obj, "output_t", 0) or 0)
        except (TypeError, ValueError):
            output_tokens = 0
        try:
            cached_input_tokens = int(getattr(usage_obj, "cache_hit_t", 0) or 0)
        except (TypeError, ValueError):
            cached_input_tokens = 0
        return {
            "calls": 1,
            "input_tokens": max(0, input_tokens),
            "output_tokens": max(0, output_tokens),
            "cached_input_tokens": max(0, cached_input_tokens),
        }

    def _set_last_usage(self, *, calls: int, input_tokens: int, output_tokens: int, cached_input_tokens: int) -> None:
        self._last_usage = {
            "calls": max(0, int(calls)),
            "input_tokens": max(0, int(input_tokens)),
            "output_tokens": max(0, int(output_tokens)),
            "cached_input_tokens": max(0, int(cached_input_tokens)),
        }

    def _set_diag(
        self,
        *,
        degraded: bool | None = None,
        degraded_reason: str | None = None,
        parse_failed: bool | None = None,
        precheck_failed: bool | None = None,
        failure_stage: str | None = None,
    ) -> None:
        if degraded is not None:
            self._last_diag["degraded"] = bool(degraded)
        if degraded_reason is not None:
            self._last_diag["degraded_reason"] = str(degraded_reason)
        if parse_failed is not None:
            self._last_diag["parse_failed"] = bool(parse_failed)
        if precheck_failed is not None:
            self._last_diag["precheck_failed"] = bool(precheck_failed)
        if failure_stage is not None:
            self._last_diag["failure_stage"] = str(failure_stage)

    @staticmethod
    def _parse_json_text(text: str) -> dict[str, Any] | None:
        if not text.strip():
            return None
        text = text.strip()
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
        if fenced:
            candidate = fenced.group(1)
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                return None
        return None

    def _load_prompt(self, filename: str) -> str:
        if filename in self._prompt_cache:
            return self._prompt_cache[filename]
        path = self.prompt_dir / filename
        if not path.exists():
            content = ""
        else:
            content = path.read_text(encoding="utf-8")
        self._prompt_cache[filename] = content
        return content

    def _fallback_clean(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw = str(payload.get("raw_text", ""))
        if self._looks_like_source_code(raw):
            memory_doc = self._build_memory_doc(raw, "source_code")
            return self._accept_clean_result(
                raw,
                input_type="source_code",
                memory_doc=memory_doc,
                skip_clean=True,
                preserve_literal=True,
            )
        raw_stripped = raw.strip()
        parsed = self._try_parse_json(raw_stripped)
        input_type = self._infer_input_type(raw, parsed)
        base_text = raw
        if parsed is not None:
            extracted = self._extract_structured_text(parsed)
            if extracted:
                base_text = extracted
        clean_text = self._basic_clean(base_text)
        if self._is_noise_text(clean_text):
            return self._reject_clean_result(
                reason="input noise too high or semantic signal too low",
                input_type=input_type,
                clean_text="",
            )
        memory_doc = self._build_memory_doc(clean_text, input_type)
        return self._accept_clean_result(clean_text, input_type=input_type, memory_doc=memory_doc)

    def _fallback_ingest(self, payload: dict[str, Any]) -> dict[str, Any]:
        source_raw = str(payload.get("raw_text", ""))
        input_type = str(payload.get("input_type", "")).strip().lower()
        preserve_literal = bool(payload.get("preserve_literal", False))
        skip_clean = bool(payload.get("skip_clean", False))

        default_weight = payload.get("default_weight", 0.7)
        try:
            base_weight = float(default_weight)
        except (TypeError, ValueError):
            base_weight = 0.7
        base_weight = max(0.0, min(1.0, base_weight))

        if input_type == "source_code" or preserve_literal or skip_clean:
            raw_text = source_raw
        else:
            raw_text = source_raw.strip()
            if not raw_text:
                raw_text = "(empty)"
        title = raw_text[:40]
        summary = raw_text[:120]
        return {
            "kind": "memory",
            "title": title,
            "summary": summary,
            "content": raw_text,
            "weight": base_weight,
            "event": str(payload.get("event", "ADD")),
            "gray": False,
            "expires_at": None,
            "relations": {k: [] for k in normalize_relations({}).keys()},
        }

    def _fallback_query(
        self,
        payload: dict[str, Any],
        fallback_candidates: list[tuple[dict[str, Any], float]],
    ) -> dict[str, Any]:
        top_k = max(1, int(payload.get("top_k", 5)))
        picked = fallback_candidates[:top_k]
        raw_scores = [max(0.0, float(s)) for _, s in picked]
        norm_scores = self._normalize_scores(raw_scores)
        matches: list[dict[str, Any]] = []
        for idx, (record, _) in enumerate(picked):
            matches.append(
                {
                    "key": str(record.get("key", "")),
                    "score": norm_scores[idx],
                    "reason": "local rerank fallback",
                    "summary": str(record.get("summary", "")),
                    "source": "local_rerank",
                }
            )
        if matches:
            answer = f"hit {len(matches)} memories, verify evidence key first"
        else:
            answer = "no usable memory matched"
        return {"answer": answer, "matches": matches}

    @staticmethod
    def _normalize_scores(raw_scores: list[float]) -> list[float]:
        if not raw_scores:
            return []
        tau = 2.0
        return [max(0.0, min(1.0, 1.0 - math.exp(-max(0.0, float(s)) / tau))) for s in raw_scores]

    def _fallback_compress(self, payload: dict[str, Any]) -> dict[str, Any]:
        records = payload.get("records", [])
        if not isinstance(records, list):
            records = []
        drop_keys: list[str] = []
        total_keys = 0
        for rec in records:
            if not isinstance(rec, dict):
                continue
            key = str(rec.get("key", "")).strip()
            if not key:
                continue
            total_keys += 1
            if bool(rec.get("gray", False)):
                drop_keys.append(key)
        keep_count = max(0, total_keys - len(drop_keys))
        merged_summary = (
            f"fallback compress keep={keep_count} drop={len(drop_keys)} "
            f"reason={payload.get('reason', '')}"
        )
        return {
            "drop_keys": drop_keys,
            "merged_summary": merged_summary,
            "reweighted": [],
            "content_updates": [],
        }

    def _fallback_split(self, payload: dict[str, Any]) -> dict[str, Any]:
        records = payload.get("records", [])
        if not isinstance(records, list):
            records = []
        merge_groups: list[dict[str, Any]] = []
        keep_items: list[dict[str, Any]] = []
        group_count = min(max(2, int(payload.get("target_groups_min", 2))), 4)
        buckets: list[list[str]] = [[] for _ in range(group_count)]
        for idx, rec in enumerate(records):
            if not isinstance(rec, dict):
                continue
            key = str(rec.get("key", "")).strip()
            if not key:
                continue
            # Always allow bucket-like items to participate in fallback split;
            # this guarantees fallback can still break "dead buckets".
            buckets[idx % group_count].append(key)
        for idx, keys in enumerate(buckets):
            if not keys:
                continue
            merge_groups.append(
                {
                    "title": f"split_group_{idx+1}",
                    "summary": f"fallback split group {idx+1}",
                    "content": f"Auto-generated bucket detail for group {idx+1}",
                    "keys": keys,
                }
            )
        if not merge_groups and records:
            merge_groups = [
                {
                    "title": "split_group_1",
                    "summary": "fallback split group",
                    "content": "fallback bucket detail",
                    "keys": [],
                }
            ]
        return {"merge_groups": merge_groups, "keep_items": keep_items}

    def _fallback_text_chunk(self, payload: dict[str, Any]) -> dict[str, Any]:
        text = str(payload.get("raw_text", ""))
        max_chars = max(100, int(payload.get("chunk_max_chars", 4000)))
        overlap = max(0, int(payload.get("chunk_overlap_chars", 200)))
        if overlap >= max_chars:
            overlap = max_chars // 4
        chunks = self._window_split(text, max_chars=max_chars, overlap=overlap)
        return {"chunks": chunks}

    def _fallback_bucket_summary(self, payload: dict[str, Any]) -> dict[str, str]:
        records = payload.get("records", [])
        if not isinstance(records, list):
            records = []
        lines: list[str] = []
        for item in records[:20]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "")).strip()
            summary = str(item.get("summary", "")).strip()
            if title or summary:
                lines.append(f"{title}: {summary}".strip(": "))
        detail = "\\n".join(lines).strip() or "bucket summary unavailable"
        detail = detail[:1000]
        brief = detail[:140]
        return {"content": detail, "summary": brief}

    def _fallback_optimize(self, payload: dict[str, Any]) -> dict[str, Any]:
        req = payload.get("payload", {})
        if not isinstance(req, dict):
            req = {}
        parent_keys: list[str] = []
        tree = req.get("tree", {})
        if isinstance(tree, dict):
            root = tree.get("ROOT", {})
            if isinstance(root, dict):
                children = root.get("children", {})
                if isinstance(children, dict):
                    for node_id in children.keys():
                        token = str(node_id).strip()
                        if token:
                            parent_keys.append(token)
        if not parent_keys:
            parent_direct = req.get("parent_direct", [])
            if isinstance(parent_direct, list):
                for item in parent_direct:
                    if not isinstance(item, dict):
                        continue
                    node_key = str(item.get("node_key", "")).strip()
                    if node_key:
                        parent_keys.append(node_key)
        return {
            "skip_optimize": True,
            "skip_reason": "fallback_noop",
            "parent_flat_keys": parent_keys,
            "groups": [],
            "parent_summary": "",
            "parent_content": "",
            "metadata_update": {},
        }

    def _normalize_optimize_result(self, result: dict[str, Any]) -> dict[str, Any]:
        skip_optimize = bool(result.get("skip_optimize", False))
        skip_reason = str(result.get("skip_reason", "")).strip()[:240]
        parent_flat_keys: list[str] = []
        parent_raw = result.get("parent_flat_keys", [])
        if isinstance(parent_raw, list):
            for item in parent_raw:
                token = str(item).strip()
                if token:
                    parent_flat_keys.append(token)

        groups_raw = result.get("groups", [])
        groups: list[dict[str, Any]] = []
        if isinstance(groups_raw, list):
            for item in groups_raw:
                if not isinstance(item, dict):
                    continue
                members_raw = item.get("members", [])
                if not isinstance(members_raw, list):
                    members_raw = []
                members: list[str] = []
                for x in members_raw:
                    token = str(x).strip()
                    if token:
                        members.append(token)
                if not members:
                    continue
                groups.append(
                    {
                        "group_bucket_id": str(item.get("group_bucket_id", "")).strip(),
                        "title": str(item.get("title", "")).strip()[:120],
                        "summary": str(item.get("summary", "")).strip()[:280],
                        "content": str(item.get("content", "")).strip()[:1000],
                        "members": members,
                    }
                )

        parent_summary = str(result.get("parent_summary", "")).strip()[:280]
        parent_content = str(result.get("parent_content", "")).strip()[:1000]
        metadata_update_raw = result.get("metadata_update", {})
        metadata_update: dict[str, dict[str, Any]] = {}
        if isinstance(metadata_update_raw, dict):
            for raw_key, raw_value in metadata_update_raw.items():
                node_key = str(raw_key).strip()
                if not node_key or not isinstance(raw_value, dict):
                    continue
                row: dict[str, Any] = {}
                if "title" in raw_value:
                    row["title"] = str(raw_value.get("title", "")).strip()[:120]
                if "summary" in raw_value:
                    row["summary"] = str(raw_value.get("summary", "")).strip()[:280]
                if "content" in raw_value:
                    row["content"] = str(raw_value.get("content", "")).strip()[:1000]
                if "relations" in raw_value:
                    row["relations"] = normalize_relations(raw_value.get("relations", {}))
                if row:
                    metadata_update[node_key] = row
        return {
            "skip_optimize": skip_optimize,
            "skip_reason": skip_reason,
            "parent_flat_keys": parent_flat_keys,
            "groups": groups,
            "parent_summary": parent_summary,
            "parent_content": parent_content,
            "metadata_update": metadata_update,
        }

    def _normalize_clean_result(self, result: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        raw_text = str(payload.get("raw_text", ""))
        parsed = self._try_parse_json(raw_text)
        fallback_type = self._infer_input_type(raw_text, parsed)

        input_type = str(result.get("input_type", "")).strip().lower() or fallback_type
        if input_type not in {"plain", "json", "chat_event", "log", "source_code", "unknown"}:
            input_type = fallback_type

        skip_clean = bool(result.get("skip_clean", False))
        preserve_literal = bool(result.get("preserve_literal", False))
        if input_type == "source_code":
            skip_clean = True
            preserve_literal = True
        if preserve_literal:
            skip_clean = True

        if skip_clean:
            clean_text = raw_text
        else:
            clean_text = self._basic_clean(str(result.get("clean_text", "")).strip())
            if not clean_text:
                clean_text = self._basic_clean(raw_text)

        accept_raw = result.get("accept", True)
        accept = bool(accept_raw)
        reject_reason = str(result.get("reject_reason", "")).strip()

        memory_doc = result.get("memory_doc")
        if not isinstance(memory_doc, dict):
            memory_doc = self._build_memory_doc(clean_text, input_type)

        if accept and (not skip_clean) and self._is_noise_text(clean_text):
            accept = False
            if not reject_reason:
                reject_reason = "input noise too high or semantic signal too low"

        if not accept:
            if not reject_reason:
                reject_reason = "clean model rejected input"
            return self._reject_clean_result(
                reason=reject_reason,
                input_type=input_type,
                clean_text=clean_text,
                memory_doc=memory_doc,
                skip_clean=skip_clean,
                preserve_literal=preserve_literal,
            )
        return self._accept_clean_result(
            clean_text,
            input_type=input_type,
            memory_doc=memory_doc,
            skip_clean=skip_clean,
            preserve_literal=preserve_literal,
        )

    def _normalize_ingest_result(self, result: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        title = str(result.get("title", "")).strip() or str(payload.get("raw_text", ""))[:40]
        summary = str(result.get("summary", "")).strip() or str(payload.get("raw_text", ""))[:120]
        source_raw = str(payload.get("raw_text", ""))
        # metadata-only ingest: content is always injected from upstream payload.
        content = source_raw
        default_weight = payload.get("default_weight", 0.5)
        try:
            weight_default = float(default_weight)
        except (TypeError, ValueError):
            weight_default = 0.5
        try:
            weight = float(result.get("weight", weight_default))
        except (TypeError, ValueError):
            weight = weight_default
        weight = max(0.0, min(1.0, weight))
        gray = bool(result.get("gray", False))
        expires_at = result.get("expires_at")
        if expires_at is not None:
            expires_at = str(expires_at)
        return {
            "kind": str(result.get("kind", "memory")),
            "title": title,
            "summary": summary[:300],
            "content": content,
            "weight": weight,
            "event": str(result.get("event", payload.get("event", "ADD"))),
            "gray": gray,
            "relations": normalize_relations(result.get("relations", {})),
            "expires_at": expires_at,
        }

    def _normalize_query_result(self, result: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        answer = str(result.get("answer", "")).strip() or "no usable memory matched"
        raw_matches = result.get("matches", [])
        matches: list[dict[str, Any]] = []
        if isinstance(raw_matches, list):
            for item in raw_matches:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("key", "")).strip()
                if not key:
                    continue
                try:
                    score = float(item.get("score", 0.0))
                except (TypeError, ValueError):
                    score = 0.0
                score = max(0.0, min(1.0, score))
                matches.append(
                    {
                        "key": key,
                        "score": score,
                        "reason": str(item.get("reason", "")),
                        "summary": str(item.get("summary", "")),
                    }
                )

        if not matches:
            return {"answer": answer, "matches": []}
        return {"answer": answer, "matches": matches}

    def _normalize_compress_result(self, result: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        drop_keys = result.get("drop_keys", [])
        if not isinstance(drop_keys, list):
            drop_keys = []
        drop = [str(k) for k in drop_keys if str(k).strip()]
        merged_summary = str(result.get("merged_summary", "")).strip()
        if not merged_summary:
            merged_summary = self._fallback_compress(payload)["merged_summary"]

        reweighted_raw = result.get("reweighted", [])
        reweighted: list[dict[str, Any]] = []
        if isinstance(reweighted_raw, list):
            for item in reweighted_raw:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("key", "")).strip()
                if not key:
                    continue
                try:
                    weight = float(item.get("weight", 0.5))
                except (TypeError, ValueError):
                    continue
                reweighted.append({"key": key, "weight": max(0.0, min(1.0, weight))})

        updates_raw = result.get("content_updates", [])
        updates: list[dict[str, Any]] = []
        if isinstance(updates_raw, list):
            for item in updates_raw:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("key", "")).strip()
                content = str(item.get("content", "")).strip()
                reason = str(item.get("reason", "")).strip()
                if not key or not content or not reason:
                    continue
                updates.append({"key": key, "content": content, "reason": reason})

        return {
            "drop_keys": drop,
            "merged_summary": merged_summary,
            "reweighted": reweighted,
            "content_updates": updates,
        }

    def _normalize_split_result(self, result: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        def _norm_items(raw_items: Any, *, default_title: str, default_summary: str, default_content: str) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            if not isinstance(raw_items, list):
                return out
            for item in raw_items:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                summary = str(item.get("summary", "")).strip()
                content = str(item.get("content", "")).strip()
                keys_raw = item.get("keys", [])
                if not isinstance(keys_raw, list):
                    keys_raw = []
                keys = [str(k).strip() for k in keys_raw if str(k).strip()]
                if not keys:
                    continue
                out.append(
                    {
                        "title": title or default_title,
                        "summary": (summary or default_summary)[:140],
                        "content": (content or summary or title or default_content)[:1000],
                        "keys": keys,
                    }
                )
            return out

        merge_groups = _norm_items(
            result.get("merge_groups", []),
            default_title="merge_group",
            default_summary="merge group",
            default_content="merge group detail",
        )
        keep_items = _norm_items(
            result.get("keep_items", []),
            default_title="keep_item",
            default_summary="kept item",
            default_content="kept item detail",
        )

        # Backward compatibility for older schema {"groups":[...]}.
        if not merge_groups and not keep_items:
            legacy_groups = _norm_items(
                result.get("groups", []),
                default_title="split_group",
                default_summary="split group",
                default_content="split group detail",
            )
            merge_groups = legacy_groups

        if not merge_groups and not keep_items:
            return self._fallback_split(payload)
        return {"merge_groups": merge_groups, "keep_items": keep_items}

    def _normalize_text_chunk_result(self, result: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        raw_chunks = result.get("chunks", [])
        if not isinstance(raw_chunks, list):
            return self._fallback_text_chunk(payload)

        line_list_raw = payload.get("line_list", [])
        if not isinstance(line_list_raw, list):
            return self._fallback_text_chunk(payload)
        line_list = [str(x) for x in line_list_raw]
        total_lines = len(line_list)
        if total_lines <= 0:
            return self._fallback_text_chunk(payload)

        fallback_all = self._fallback_text_chunk(payload).get("chunks", [])
        fallback_all = [str(x).strip() for x in fallback_all if str(x).strip()]

        valid_chunks: list[tuple[int, str]] = []
        covered_lines: set[int] = set()
        invalid_count = 0
        total_declared = len(raw_chunks)

        for item in raw_chunks:
            if not isinstance(item, dict):
                invalid_count += 1
                continue
            ranges_raw = item.get("ranges", [])
            if not isinstance(ranges_raw, list) or not ranges_raw:
                invalid_count += 1
                continue
            replacements_raw = item.get("replacements", {})
            if not isinstance(replacements_raw, dict):
                invalid_count += 1
                continue

            ordered_lines: list[int] = []
            chunk_lines_seen: set[int] = set()
            range_start_min: int | None = None
            range_ok = True

            for r in ranges_raw:
                if not isinstance(r, list) or len(r) != 2:
                    range_ok = False
                    break
                try:
                    start_line = int(r[0])
                    end_line = int(r[1])
                except (TypeError, ValueError):
                    range_ok = False
                    break
                if start_line < 1 or end_line < 1 or start_line > end_line:
                    range_ok = False
                    break
                if end_line > total_lines:
                    range_ok = False
                    break
                if range_start_min is None or start_line < range_start_min:
                    range_start_min = start_line
                for ln in range(start_line, end_line + 1):
                    if ln in chunk_lines_seen:
                        range_ok = False
                        break
                    chunk_lines_seen.add(ln)
                    ordered_lines.append(ln)
                if not range_ok:
                    break

            if not range_ok or not ordered_lines:
                invalid_count += 1
                continue

            replacements: dict[int, str] = {}
            replacement_ok = True
            for k, v in replacements_raw.items():
                try:
                    ln = int(str(k))
                except (TypeError, ValueError):
                    replacement_ok = False
                    break
                if ln < 1 or ln > total_lines:
                    replacement_ok = False
                    break
                if ln not in chunk_lines_seen:
                    replacement_ok = False
                    break
                replacements[ln] = str(v)
            if not replacement_ok:
                invalid_count += 1
                continue

            out_lines: list[str] = []
            for ln in ordered_lines:
                if ln in replacements:
                    rep = replacements[ln]
                    if rep == "":
                        continue
                    if "\n" in rep:
                        out_lines.extend(rep.split("\n"))
                    else:
                        out_lines.append(rep)
                else:
                    out_lines.append(line_list[ln - 1])

            chunk_text = "\n".join(out_lines).strip()
            if not chunk_text:
                invalid_count += 1
                continue

            valid_chunks.append((range_start_min or 1, chunk_text))
            covered_lines.update(chunk_lines_seen)

        if total_declared <= 0:
            return self._fallback_text_chunk(payload)

        invalid_ratio = invalid_count / max(1, total_declared)
        if invalid_ratio >= 0.5:
            return {"chunks": fallback_all}

        merged: list[tuple[int, str]] = list(valid_chunks)
        uncovered_segments = self._uncovered_segments(total_lines=total_lines, covered=covered_lines)
        for start_line, end_line in uncovered_segments:
            segment_text = "\n".join(line_list[start_line - 1 : end_line]).strip()
            if not segment_text:
                continue
            for piece in self._window_split(
                segment_text,
                max_chars=max(100, int(payload.get("chunk_max_chars", 4000))),
                overlap=max(0, int(payload.get("chunk_overlap_chars", 200))),
            ):
                piece = str(piece).strip()
                if piece:
                    merged.append((start_line, piece))

        if not merged:
            return {"chunks": fallback_all}

        merged.sort(key=lambda x: x[0])
        out_chunks = [text for _, text in merged if text.strip()]
        if not out_chunks:
            return {"chunks": fallback_all}
        return {"chunks": out_chunks}

    @staticmethod
    def _normalize_bucket_summary(result: dict[str, Any]) -> dict[str, str]:
        content = str(result.get("content", "")).strip()[:1000]
        summary = str(result.get("summary", "")).strip()[:140]
        if not content:
            content = summary or "bucket summary unavailable"
        if not summary:
            summary = content[:140]
        return {"content": content, "summary": summary}

    @staticmethod
    def _accept_clean_result(
        clean_text: str,
        *,
        input_type: str,
        memory_doc: dict[str, Any] | None = None,
        skip_clean: bool = False,
        preserve_literal: bool = False,
    ) -> dict[str, Any]:
        if memory_doc is None:
            memory_doc = {}
        return {
            "accept": True,
            "reject_reason": "",
            "input_type": input_type,
            "skip_clean": bool(skip_clean),
            "preserve_literal": bool(preserve_literal),
            "clean_text": clean_text,
            "memory_doc": memory_doc,
        }

    @staticmethod
    def _reject_clean_result(
        *,
        reason: str,
        input_type: str,
        clean_text: str = "",
        memory_doc: dict[str, Any] | None = None,
        skip_clean: bool = False,
        preserve_literal: bool = False,
    ) -> dict[str, Any]:
        if memory_doc is None:
            memory_doc = {}
        return {
            "accept": False,
            "reject_reason": reason.strip() or "clean rejected input",
            "input_type": input_type,
            "skip_clean": bool(skip_clean),
            "preserve_literal": bool(preserve_literal),
            "clean_text": clean_text,
            "memory_doc": memory_doc,
        }

    @staticmethod
    def _basic_clean(text: str) -> str:
        return re.sub(r"\s+", " ", text or "").strip()

    @staticmethod
    def _try_parse_json(raw_text: str) -> Any:
        if not raw_text:
            return None
        stripped = raw_text.strip()
        if not stripped.startswith("{") and not stripped.startswith("["):
            return None
        try:
            return json.loads(stripped)
        except Exception:
            return None

    @classmethod
    def _infer_input_type(cls, raw_text: str, parsed: Any) -> str:
        if cls._looks_like_source_code(raw_text):
            return "source_code"
        if parsed is not None:
            if isinstance(parsed, dict):
                keys = {str(k).lower() for k in parsed.keys()}
                chatish = {"message", "content", "sender", "role", "event", "chat", "channel", "user"}
                if len(keys.intersection(chatish)) >= 2:
                    return "chat_event"
            return "json"
        if "\n" in raw_text and re.search(r"\d{4}[-/]\d{2}[-/]\d{2}", raw_text):
            return "log"
        if raw_text.strip():
            return "plain"
        return "unknown"

    @staticmethod
    def _looks_like_source_code(text: str) -> bool:
        t = str(text or "")
        if not t.strip():
            return False

        score = 0
        if re.search(r"^\s*(def|async\s+def|class)\s+[A-Za-z_]\w*", t, re.M):
            score += 3
        if re.search(r"^\s*(import|from)\s+[A-Za-z_][\w\.]*", t, re.M):
            score += 3
        if re.search(r"^\s*if\s+__name__\s*==\s*['\"]__main__['\"]\s*:", t, re.M):
            score += 3
        if re.search(r"^\s*#include\s+<[^>]+>", t, re.M):
            score += 3
        if re.search(r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER)\b", t, re.I | re.M):
            score += 2
        if re.search(r"[{}();]{2,}|=>|::|:=|==|!=", t):
            score += 1
        if "\n" in t and re.search(r"^\s{2,}\S", t, re.M):
            score += 1
        if re.search(r"`{3}", t):
            score += 1
        if re.search(r"\b(lambda|return|try|except|finally|yield)\b", t):
            score += 1

        return score >= 3

    @classmethod
    def _extract_structured_text(cls, value: Any) -> str:
        chunks: list[str] = []

        def _walk(node: Any) -> None:
            if isinstance(node, dict):
                for _, v in node.items():
                    _walk(v)
                return
            if isinstance(node, list):
                for item in node:
                    _walk(item)
                return
            if isinstance(node, (str, int, float, bool)):
                text = str(node).strip()
                if text:
                    chunks.append(text)

        _walk(value)
        if not chunks:
            return ""
        merged = " ".join(chunks)
        return cls._basic_clean(merged)

    @staticmethod
    def _is_noise_text(text: str) -> bool:
        t = (text or "").strip()
        if not t:
            return True
        meaningful = re.findall(r"[A-Za-z0-9\u4e00-\u9fff]", t)
        if len(meaningful) < 2:
            return True
        symbol_count = len(re.findall(r"[^\w\s\u4e00-\u9fff]", t))
        if symbol_count / max(1, len(t)) > 0.75 and len(meaningful) < 8:
            return True
        if len(t) < 3:
            return True
        return False

    @staticmethod
    def _build_memory_doc(clean_text: str, input_type: str) -> dict[str, Any]:
        return {
            "source": input_type,
            "content": clean_text,
        }

    @staticmethod
    def _split_lines(text: str) -> list[str]:
        t = str(text or "")
        if not t:
            return []
        lines = t.splitlines()
        if not lines:
            return [t]
        return lines

    @staticmethod
    def _uncovered_segments(*, total_lines: int, covered: set[int]) -> list[tuple[int, int]]:
        segments: list[tuple[int, int]] = []
        cur_start: int | None = None
        for ln in range(1, total_lines + 1):
            if ln in covered:
                if cur_start is not None:
                    segments.append((cur_start, ln - 1))
                    cur_start = None
                continue
            if cur_start is None:
                cur_start = ln
        if cur_start is not None:
            segments.append((cur_start, total_lines))
        return segments

    @staticmethod
    def _window_split(text: str, *, max_chars: int, overlap: int) -> list[str]:
        raw = str(text or "")
        if not raw:
            return []
        if len(raw) <= max_chars:
            return [raw]
        step = max(1, max_chars - overlap)
        chunks: list[str] = []
        start = 0
        n = len(raw)
        while start < n:
            end = min(n, start + max_chars)
            chunk = raw[start:end].strip()
            if chunk:
                chunks.append(chunk)
            if end >= n:
                break
            start += step
        return chunks

