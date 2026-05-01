from __future__ import annotations

from typing import Any

from ..aliasing import AliasPayloadError, looks_like_alias
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


class QueryService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

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
    ) -> QueryResult:
        eng = self.runtime.engine
        root = eng._resolve_bucket_id(bucket_id)
        visited: set[str] = set()
        depth_limit = max_depth if max_depth is not None else eng._max_depth + 2
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
        )
        degraded_cache_key = eng.storage.compute_cache_key(
            query_text=query_text,
            top_k=top_k,
            include_gray=include_gray,
            bucket_id=bucket_id,
            degraded_mode=True,
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
            if rec.key in hint_seen:
                continue
            hint_seen.add(rec.key)
            hint_keys.append(rec.key)
            if len(hint_keys) >= 50:
                break
        if not hint_keys:
            hint_keys = [r.key for r in records[:50]]
        alias_key_hints: list[str] = []
        for key in hint_keys:
            token = eng.storage.find_alias(bucket_id, key, "memory")
            if token:
                alias_key_hints.append(token)
            else:
                alias_miss_build += 1
        if alias_miss_build > 0:
            eng._enqueue_query_side_effect("record_alias_miss_build", {"count": alias_miss_build})
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
        query_matches, llm_accepted_count = self.merge_llm_bm25_matches(
            records=records,
            llm_matches=llm_result.get("matches", []),
            bm25_ranked=bm25_ranked,
            bm25_norm_map=bm25_norm_map,
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
        )
        recall_keys = [m.key for m in final_matches if str(m.key).strip()]
        if recall_keys:
            eng._enqueue_query_side_effect("record_recall_batch", {"keys": recall_keys})

        top_matches = final_matches[:top_k]
        has_local_supplement = any(str(m.source).lower() == "bm25" for m in top_matches)
        if degraded:
            result_source = "LOCAL"
        elif llm_accepted_count >= top_k and not has_local_supplement:
            result_source = "LLM"
        else:
            result_source = "MIX"

        result = QueryResult(
            success=True,
            answer=str(llm_result.get("answer", "")) or f"hit {len(final_matches)} memories",
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
                real_key = eng.resolve_alias(bucket_id, raw_key, expected_type="memory")
            except Exception:
                miss += 1
                continue
            out_matches.append(
                {
                    "key": real_key,
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
    ) -> tuple[list[QueryMatch], str]:
        eng = self.runtime.engine
        out: list[QueryMatch] = []
        seen: set[str] = set()
        sub_answer = ""
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
                )
                if not sub_answer:
                    sub_answer = str(child.sub_answer or child.answer or "").strip()
                if child.matches:
                    child_top = child.matches[0]
                    merged_score = _clamp_score(0.70 * match.score + 0.30 * child_top.score)
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
