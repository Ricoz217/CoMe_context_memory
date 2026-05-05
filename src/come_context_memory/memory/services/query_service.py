from __future__ import annotations

import re
import time
from collections import deque
from typing import Any

from ..aliasing import AliasPayloadError, infer_real_key_type, looks_like_alias
from ..models import (
    BUCKET_KIND_BUCKET,
    BUCKET_KIND_MEMORY,
    MemoryRecord,
    QueryMatch,
    QueryResult,
)
from ..rerank import normalize_scores, rank_records_with_index
from .runtime import ServiceRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


_LITERAL_HINT_PATTERN = re.compile(r"[`/\\._:#()\[\]{}<>]|[A-Za-z0-9_]+\.[A-Za-z0-9_]+")


class QueryService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    @staticmethod
    def _node_view_key(rec: MemoryRecord) -> str:
        if rec.kind == BUCKET_KIND_BUCKET:
            child = str(rec.child_bucket_id or "").strip()
            if child:
                return child
        return rec.key

    async def run_query(
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
        eng = self.runtime.engine
        root = eng._resolve_bucket_id(bucket_id)
        visited: set[str] = set()
        depth_limit = max_depth if max_depth is not None else eng._max_depth + 2
        mode_effective = self._resolve_query_mode(mode, query_text, eng._query_mode_default)
        recall_top_n = max(10, int(global_recall_top_n if global_recall_top_n is not None else eng._global_recall_top_n))
        recall_top_m = max(1, int(global_recall_top_m if global_recall_top_m is not None else eng._global_recall_top_m))
        recall_depth_limit = max(
            1,
            int(
                global_recall_depth_limit
                if global_recall_depth_limit is not None
                else eng._global_recall_depth_limit
            ),
        )
        recall_time_budget_ms = max(
            10,
            int(
                global_recall_time_budget_ms
                if global_recall_time_budget_ms is not None
                else eng._global_recall_time_budget_ms
            ),
        )
        global_record_boost, global_bucket_boost = self._build_global_recall_boosts(
            root_bucket_id=root,
            query_text=query_text,
            include_gray=include_gray,
            top_n=recall_top_n,
            top_m=recall_top_m,
            depth_limit=recall_depth_limit,
            time_budget_ms=recall_time_budget_ms,
        )
        result = await self.query_bucket_recursive(
            bucket_id=root,
            query_text=query_text,
            top_k=max(1, int(top_k)),
            include_gray=include_gray,
            use_cache=use_cache,
            with_evidence=with_evidence,
            depth=1,
            depth_limit=max(1, int(depth_limit)),
            visited=visited,
            mode=mode_effective,
            global_recall_top_n=recall_top_n,
            global_recall_top_m=recall_top_m,
            global_recall_depth_limit=recall_depth_limit,
            global_recall_time_budget_ms=recall_time_budget_ms,
            global_record_boost=global_record_boost,
            global_bucket_boost=global_bucket_boost,
        )
        return result

    async def query_bucket_recursive(
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
        mode: str,
        global_recall_top_n: int,
        global_recall_top_m: int,
        global_recall_depth_limit: int,
        global_recall_time_budget_ms: int,
        global_record_boost: dict[str, float],
        global_bucket_boost: dict[str, float],
    ) -> QueryResult:
        eng = self.runtime.engine
        if bucket_id in visited:
            return QueryResult(
                success=True,
                answer="recursive bucket cycle detected",
                matches=[],
                result_source="LOCAL",
                cache_hit=False,
                include_gray_used=include_gray,
                degraded=False,
                degraded_reason="",
                failure_stage="",
                sub_answer="",
                message="cycle",
            )
        visited.add(bucket_id)

        meta = eng.storage.load_meta()
        normal_cache_key = eng.storage.compute_cache_key(
            query_text=query_text,
            top_k=top_k,
            include_gray=include_gray,
            bucket_id=bucket_id,
            degraded_mode=False,
            mode=mode,
            global_recall_top_n=global_recall_top_n,
            global_recall_top_m=global_recall_top_m,
            global_recall_depth_limit=global_recall_depth_limit,
            global_recall_time_budget_ms=global_recall_time_budget_ms,
        )
        degraded_cache_key = eng.storage.compute_cache_key(
            query_text=query_text,
            top_k=top_k,
            include_gray=include_gray,
            bucket_id=bucket_id,
            degraded_mode=True,
            mode=mode,
            global_recall_top_n=global_recall_top_n,
            global_recall_top_m=global_recall_top_m,
            global_recall_depth_limit=global_recall_depth_limit,
            global_recall_time_budget_ms=global_recall_time_budget_ms,
        )
        if use_cache and not bool(meta.get("dirty", False)):
            hit = eng.storage.get_query_cache(normal_cache_key)
            if hit is None:
                hit = eng.storage.get_query_cache(degraded_cache_key)
            if isinstance(hit, dict) and isinstance(hit.get("result"), dict):
                result = QueryResult.from_dict(hit["result"])
                result.cache_hit = True
                return result

        records = eng.storage.list_bucket_records(bucket_id, include_gray=include_gray)
        if not records:
            empty = QueryResult(
                success=True,
                answer="no memory in bucket",
                matches=[],
                result_source="LOCAL",
                cache_hit=False,
                include_gray_used=include_gray,
                degraded=False,
                degraded_reason="",
                failure_stage="",
                sub_answer="",
                message="empty",
            )
            eng.storage.set_query_cache(normal_cache_key, empty.to_dict(), bucket_id=bucket_id)
            return empty

        bucket_version = eng.storage.get_bucket_version(bucket_id)
        bm25_index = eng.bm25_cache.get_or_build(
            bucket_id=bucket_id,
            bucket_version=bucket_version,
            records=records,
        )
        bm25_ranked = rank_records_with_index(
            query_text,
            records,
            top_k=max(top_k * 6, top_k),
            index=bm25_index,
        )
        bm25_raw_scores = [score for _, score in bm25_ranked]
        bm25_norm_scores = normalize_scores(bm25_raw_scores)
        bm25_norm_map = {rec.key: bm25_norm_scores[idx] for idx, (rec, _) in enumerate(bm25_ranked)}
        boosted_norm_map: dict[str, float] = {}
        for rec, _score in bm25_ranked:
            boosted_norm_map[rec.key] = self._apply_global_boost(
                rec=rec,
                base_score=float(bm25_norm_map.get(rec.key, 0.0)),
                global_record_boost=global_record_boost,
                global_bucket_boost=global_bucket_boost,
                boost_weight=float(getattr(eng, "_global_recall_boost_weight", 0.0)),
            )
        node_key_to_record_key: dict[str, str] = {}
        for rec in records:
            node_key_to_record_key[rec.key] = rec.key
            if rec.kind == BUCKET_KIND_BUCKET:
                child = str(rec.child_bucket_id or "").strip()
                if child:
                    node_key_to_record_key[child] = rec.key
        alias_fallback_candidates: list[tuple[dict[str, Any], float]] = []
        alias_miss_build = 0
        for rec, score in bm25_ranked:
            try:
                alias_rec = eng.build_llm_view(bucket_id, rec.to_dict(), allow_create=False)
            except AliasPayloadError:
                alias_miss_build += 1
                continue
            alias_fallback_candidates.append((alias_rec, score))
        hint_keys: list[str] = []
        hint_seen: set[str] = set()
        for rec, _score in bm25_ranked:
            node_key = self._node_view_key(rec)
            if node_key in hint_seen:
                continue
            hint_seen.add(node_key)
            hint_keys.append(node_key)
            if len(hint_keys) >= 50:
                break
        if not hint_keys:
            hint_keys = [self._node_view_key(r) for r in records[:50]]
        alias_key_hints: list[str] = []
        for key in hint_keys:
            key_type = infer_real_key_type(key)
            if not key_type:
                alias_miss_build += 1
                continue
            token = eng.storage.find_alias(bucket_id, key, key_type)
            if token:
                alias_key_hints.append(token)
            else:
                alias_miss_build += 1
        if alias_miss_build > 0:
            eng._enqueue_query_side_effect("record_alias_miss_build", {"count": alias_miss_build})
        if mode == "literal":
            llm_result = {"answer": "", "matches": []}
            llm_accepted_count = 0
            degraded = False
            degraded_reason = ""
            failure_stage = ""
        else:
            map_ver = eng.alias_map_version(bucket_id)
            query_alias_payload = {
                "query_text": query_text,
                "top_k": top_k,
                "include_gray": include_gray,
                "key_hints": alias_key_hints,
                "hint_count": len(alias_key_hints),
            }
            eng.assert_alias_only_payload(bucket_id, query_alias_payload)
            llm_result_alias = await eng.pipeline.query(
                bucket_context=eng._bucket_context(bucket_id),
                query_text=query_text,
                top_k=top_k,
                include_gray=include_gray,
                key_hints=alias_key_hints,
                fallback_candidates=alias_fallback_candidates,
            )
            eng._audit_alias_llm_call(
                tool="query",
                bucket_id=bucket_id,
                map_version=map_ver,
                alias_input=query_alias_payload,
                alias_output=llm_result_alias,
            )
            llm_result, alias_miss_resolve = self._resolve_query_llm_output(
                eng=eng,
                bucket_id=bucket_id,
                llm_result_alias=llm_result_alias,
                node_key_to_record_key=node_key_to_record_key,
                record_keys=set(r.key for r in records),
            )
            if alias_miss_resolve > 0:
                eng._enqueue_query_side_effect("record_alias_miss_resolve", {"count": alias_miss_resolve})

            eng._enqueue_query_side_effect("record_llm_usage", {"usage": dict(eng.pipeline.last_usage)})
            eng._enqueue_query_side_effect("record_llm_diag", {"diag": dict(eng.pipeline.last_diagnostics)})
            diag = eng.pipeline.last_diagnostics
            degraded = bool(diag.get("degraded", False))
            degraded_reason = str(diag.get("degraded_reason", ""))
            failure_stage = eng._diag_failure_stage(diag)
            if degraded:
                eng._enqueue_query_side_effect("record_query_degraded", {})
            if eng._is_context_overflow_diag(diag):
                eng._enqueue_query_side_effect("record_overflow_query", {})
            llm_accepted_count = 0
        query_matches, llm_accepted_count = self.merge_llm_bm25_matches(
            records=records,
            llm_matches=llm_result.get("matches", []),
            bm25_ranked=bm25_ranked,
            bm25_norm_map=boosted_norm_map,
            top_k=top_k,
        )

        final_matches, sub_answer = await self.resolve_bucket_matches(
            query_text=query_text,
            query_matches=query_matches,
            parent_top_k=top_k,
            include_gray=include_gray,
            use_cache=use_cache,
            with_evidence=with_evidence,
            depth=depth,
            depth_limit=depth_limit,
            visited=visited,
            mode=mode,
            global_recall_top_n=global_recall_top_n,
            global_recall_top_m=global_recall_top_m,
            global_recall_depth_limit=global_recall_depth_limit,
            global_recall_time_budget_ms=global_recall_time_budget_ms,
            global_record_boost=global_record_boost,
            global_bucket_boost=global_bucket_boost,
        )
        recall_keys = [m.key for m in final_matches if str(m.key).strip()]
        if recall_keys:
            eng._enqueue_query_side_effect("record_recall_batch", {"keys": recall_keys})

        top_matches = final_matches[:top_k]
        has_local_supplement = any(str(m.source).lower() == "bm25" for m in top_matches)
        if mode == "literal":
            result_source = "LOCAL"
        elif degraded:
            result_source = "LOCAL"
        elif llm_accepted_count >= top_k and not has_local_supplement:
            result_source = "LLM"
        else:
            result_source = "MIX"

        result = QueryResult(
            success=True,
            answer=str(llm_result.get("answer", "")).strip() or self._build_local_answer(query_text, final_matches),
            matches=top_matches,
            result_source=result_source,
            cache_hit=False,
            include_gray_used=include_gray,
            degraded=degraded,
            degraded_reason=degraded_reason,
            failure_stage=failure_stage,
            sub_answer=sub_answer,
            message="ok",
        )

        cache_key = degraded_cache_key if degraded else normal_cache_key
        eng._enqueue_query_side_effect(
            "set_query_cache",
            {
                "cache_key": cache_key,
                "result": result.to_dict(),
                "bucket_id": bucket_id,
            },
        )
        return result

    def _resolve_query_llm_output(
        self,
        *,
        eng: Any,
        bucket_id: str,
        llm_result_alias: dict[str, Any],
        node_key_to_record_key: dict[str, str],
        record_keys: set[str],
    ) -> tuple[dict[str, Any], int]:
        answer = str(llm_result_alias.get("answer", "")).strip()
        raw_matches = llm_result_alias.get("matches", [])
        out_matches: list[dict[str, Any]] = []
        miss = 0
        if not isinstance(raw_matches, list):
            raw_matches = []
        for item in raw_matches:
            if not isinstance(item, dict):
                continue
            raw_key = str(item.get("key", "")).strip()
            if not raw_key:
                continue
            if not looks_like_alias(raw_key):
                miss += 1
                continue
            try:
                real_key = eng.resolve_alias(bucket_id, raw_key, expected_type=None)
            except Exception:
                miss += 1
                continue
            record_key = node_key_to_record_key.get(real_key, real_key)
            if record_key not in record_keys:
                miss += 1
                continue
            out_matches.append(
                {
                    "key": record_key,
                    "score": item.get("score", 0.0),
                    "reason": item.get("reason", ""),
                    "summary": item.get("summary", ""),
                }
            )
        return {"answer": answer, "matches": out_matches}, miss

    def merge_llm_bm25_matches(
        self,
        *,
        records: list[MemoryRecord],
        llm_matches: Any,
        bm25_ranked: list[tuple[MemoryRecord, float]],
        bm25_norm_map: dict[str, float],
        top_k: int,
    ) -> tuple[list[QueryMatch], int]:
        eng = self.runtime.engine
        record_map = {r.key: r for r in records}
        used: set[str] = set()
        merged: list[QueryMatch] = []
        llm_accepted_count = 0

        if isinstance(llm_matches, list):
            for item in llm_matches:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("key", "")).strip()
                rec = record_map.get(key)
                if rec is None or key in used:
                    continue
                llm_score = _clamp_score(float(item.get("score", 0.0)))
                bm_score = _clamp_score(float(bm25_norm_map.get(key, 0.0)))
                final = _clamp_score(0.85 * llm_score + 0.15 * bm_score)
                final = eng._apply_negative_weight_adjust(key, final)
                merged.append(
                    QueryMatch(
                        key=key,
                        score=final,
                        reason=str(item.get("reason", "")).strip() or "llm",
                        summary=str(item.get("summary", "")).strip() or rec.summary,
                        source="llm",
                        llm_score=llm_score,
                        bm25_score=bm_score,
                        final_score=final,
                    )
                )
                used.add(key)
                llm_accepted_count += 1

        supplement_limit = max(1, top_k * 2)
        for rec, _raw in bm25_ranked[:supplement_limit]:
            if rec.key in used:
                continue
            bm_norm = _clamp_score(float(bm25_norm_map.get(rec.key, 0.0)))
            supplement = min(0.5, bm_norm * 0.5)
            supplement = eng._apply_negative_weight_adjust(rec.key, supplement)
            merged.append(
                QueryMatch(
                    key=rec.key,
                    score=supplement,
                    reason="bm25 supplement",
                    summary=rec.summary,
                    source="bm25",
                    llm_score=0.0,
                    bm25_score=bm_norm,
                    final_score=supplement,
                )
            )
            used.add(rec.key)
            if len(merged) >= max(top_k * 3, top_k):
                break

        merged.sort(key=lambda m: m.score, reverse=True)
        return merged, llm_accepted_count

    async def resolve_bucket_matches(
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
        mode: str,
        global_recall_top_n: int,
        global_recall_top_m: int,
        global_recall_depth_limit: int,
        global_recall_time_budget_ms: int,
        global_record_boost: dict[str, float],
        global_bucket_boost: dict[str, float],
    ) -> tuple[list[QueryMatch], str]:
        eng = self.runtime.engine
        out: list[QueryMatch] = []
        seen: set[str] = set()
        sub_answer = ""
        best_sub_answer_score = -1.0
        max_parent_candidates = max(1, int(parent_top_k))
        for match in query_matches[:max_parent_candidates]:
            rec = eng.storage.get_record(match.key)
            if rec is None:
                continue
            if rec.kind == BUCKET_KIND_BUCKET and rec.child_bucket_id and depth < depth_limit:
                child_raw_id = str(rec.child_bucket_id).strip()
                if not child_raw_id:
                    continue
                child_info = eng.storage.get_bucket_info(child_raw_id)
                if child_info is not None and child_info.sealed and not str(child_info.sealed_to or "").strip():
                    continue
                try:
                    child_bucket_id = eng._resolve_bucket_id(child_raw_id)
                except ValueError:
                    continue
                child_info_final = eng.storage.get_bucket_info(child_bucket_id)
                if child_info_final is not None and child_info_final.sealed:
                    continue
                child = await self.query_bucket_recursive(
                    bucket_id=child_bucket_id,
                    query_text=query_text,
                    top_k=1,
                    include_gray=include_gray,
                    use_cache=use_cache,
                    with_evidence=with_evidence,
                    depth=depth + 1,
                    depth_limit=depth_limit,
                    visited=visited,
                    mode=mode,
                    global_recall_top_n=global_recall_top_n,
                    global_recall_top_m=global_recall_top_m,
                    global_recall_depth_limit=global_recall_depth_limit,
                    global_recall_time_budget_ms=global_recall_time_budget_ms,
                    global_record_boost=global_record_boost,
                    global_bucket_boost=global_bucket_boost,
                )
                if child.matches:
                    child_top = child.matches[0]
                    merged_score = _clamp_score(0.70 * match.score + 0.30 * child_top.score)
                    candidate_sub_answer = str(child.sub_answer or child.answer or "").strip()
                    if candidate_sub_answer and merged_score > best_sub_answer_score:
                        sub_answer = candidate_sub_answer
                        best_sub_answer_score = merged_score
                    if child_top.key not in seen:
                        out.append(
                            QueryMatch(
                                key=child_top.key,
                                score=merged_score,
                                reason=f"via_bucket:{match.key}",
                                summary=child_top.summary,
                                source="recursive",
                                llm_score=child_top.llm_score,
                                bm25_score=child_top.bm25_score,
                                final_score=merged_score,
                            )
                        )
                        seen.add(child_top.key)
                continue

            if rec.kind != BUCKET_KIND_MEMORY:
                continue
            if with_evidence and rec.evidence_ref:
                summary = f"{match.summary}\n[evidence_ref={rec.evidence_ref}]".strip()
            else:
                summary = match.summary
            if rec.key in seen:
                continue
            out.append(
                QueryMatch(
                    key=rec.key,
                    score=match.score,
                    reason=match.reason,
                    summary=summary,
                    source=match.source,
                    llm_score=match.llm_score,
                    bm25_score=match.bm25_score,
                    final_score=match.final_score,
                )
            )
            seen.add(rec.key)

        out.sort(key=lambda m: m.score, reverse=True)
        return out, sub_answer

    @staticmethod
    def _resolve_query_mode(mode: str, query_text: str, default_mode: str) -> str:
        wanted = str(mode or "").strip().lower()
        if wanted in {"semantic", "literal", "hybrid"}:
            return wanted
        fallback = str(default_mode or "auto").strip().lower()
        if wanted != "auto" and fallback in {"semantic", "literal", "hybrid"}:
            return fallback
        text = str(query_text or "")
        if len(text) >= 24 and _LITERAL_HINT_PATTERN.search(text):
            return "literal"
        if _LITERAL_HINT_PATTERN.search(text):
            return "literal"
        return "hybrid"

    def _build_global_recall_boosts(
        self,
        *,
        root_bucket_id: str,
        query_text: str,
        include_gray: bool,
        top_n: int,
        top_m: int,
        depth_limit: int,
        time_budget_ms: int,
    ) -> tuple[dict[str, float], dict[str, float]]:
        eng = self.runtime.engine
        start = time.perf_counter()
        time_budget_sec = max(0.01, float(time_budget_ms) / 1000.0)
        visited: set[str] = set()
        queue: deque[tuple[str, int]] = deque([(root_bucket_id, 1)])
        scanned: list[str] = []
        while queue and len(scanned) < max(1, int(top_n)):
            if time.perf_counter() - start > time_budget_sec:
                break
            bucket_id, depth = queue.popleft()
            if bucket_id in visited:
                continue
            visited.add(bucket_id)
            scanned.append(bucket_id)
            if depth >= max(1, int(depth_limit)):
                continue
            records = eng.storage.list_bucket_records(bucket_id, include_gray=include_gray)
            for rec in records:
                if rec.kind != BUCKET_KIND_BUCKET:
                    continue
                child = str(rec.child_bucket_id or "").strip()
                if not child:
                    continue
                try:
                    child = eng._resolve_bucket_id(child)
                except Exception:
                    pass
                if child and child not in visited:
                    queue.append((child, depth + 1))

        record_boost: dict[str, float] = {}
        bucket_boost: dict[str, float] = {}
        for bucket_id in scanned:
            if time.perf_counter() - start > time_budget_sec:
                break
            records = eng.storage.list_bucket_records(bucket_id, include_gray=include_gray)
            if not records:
                continue
            bucket_version = eng.storage.get_bucket_version(bucket_id)
            bm25_index = eng.bm25_cache.get_or_build(
                bucket_id=bucket_id,
                bucket_version=bucket_version,
                records=records,
            )
            ranked = rank_records_with_index(
                query_text,
                records,
                top_k=max(1, int(top_m)),
                index=bm25_index,
            )
            if not ranked:
                continue
            norms = normalize_scores([score for _, score in ranked])
            best = 0.0
            for idx, (rec, _raw) in enumerate(ranked):
                score = _clamp_score(float(norms[idx] if idx < len(norms) else 0.0))
                if score <= 0.0:
                    continue
                if score > best:
                    best = score
                prev = float(record_boost.get(rec.key, 0.0))
                if score > prev:
                    record_boost[rec.key] = score
            if best > float(bucket_boost.get(bucket_id, 0.0)):
                bucket_boost[bucket_id] = best
        return record_boost, bucket_boost

    @staticmethod
    def _apply_global_boost(
        *,
        rec: MemoryRecord,
        base_score: float,
        global_record_boost: dict[str, float],
        global_bucket_boost: dict[str, float],
        boost_weight: float,
    ) -> float:
        base = _clamp_score(base_score)
        boost = _clamp_score(float(global_record_boost.get(rec.key, 0.0)))
        if rec.kind == BUCKET_KIND_BUCKET:
            child = str(rec.child_bucket_id or "").strip()
            if child:
                boost = max(boost, _clamp_score(float(global_bucket_boost.get(child, 0.0))))
        weight = _clamp_score(boost_weight)
        if weight <= 0.0:
            return base
        return _clamp_score((1.0 - weight) * base + weight * boost)

    @staticmethod
    def _build_local_answer(query_text: str, matches: list[QueryMatch]) -> str:
        if not matches:
            return "no strong local match"
        top = matches[0]
        summary = str(top.summary or "").strip()
        if summary:
            return summary
        return f"hit {len(matches)} memories for query: {query_text}"
