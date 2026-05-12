from __future__ import annotations

import json
from typing import Any

from ..aliasing import AliasPayloadError
from ..models import BUCKET_KIND_BUCKET, BucketInfo, MemoryRecord, OptimizeResult, normalize_relations, utc_now_iso
from .runtime import ServiceRuntime


def _clamp_ratio(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


class OptimizeService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    @staticmethod
    def _node_view_key(rec: MemoryRecord) -> str:
        """LLM-facing node id: bucket nodes use bucket-id, memory nodes use memory key."""
        if rec.kind == BUCKET_KIND_BUCKET:
            child = str(rec.child_bucket_id or "").strip()
            if child:
                return child
        return rec.key

    async def optimize(self, bucket_id: str | None = None, *, reason: str = "manual_optimize") -> OptimizeResult:
        eng = self.runtime.engine
        target_bucket = eng._resolve_bucket_id(bucket_id)
        info = eng.storage.get_bucket_info(target_bucket)
        if info is None:
            return OptimizeResult(success=False, bucket_id=target_bucket, message="target bucket not found", reason_code="bucket_not_found")
        if int(info.level) >= int(eng._max_depth):
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message="third-level bucket optimize is not supported",
                reason_code="third_level_not_supported",
            )

        (
            candidate_records,
            parent_keys,
            child_expansions,
            sealed_redirects,
            leaf_node_keys,
            leaf_bucket_keys,
            bucket_id_to_node_key,
        ) = self._collect_candidates(target_bucket)
        if not candidate_records:
            return OptimizeResult(success=False, bucket_id=target_bucket, message="no candidates in target bucket", reason_code="empty_bucket")

        payload = self._build_optimize_payload(
            target_bucket=target_bucket,
            info=info,
            parent_keys=parent_keys,
            child_expansions=child_expansions,
            candidate_records=candidate_records,
            leaf_nodes=leaf_node_keys,
            reason=reason,
        )
        est_tokens = max(1, len(json.dumps(payload, ensure_ascii=False)) // 3)
        if est_tokens > int(eng.max_context_window * 0.70):
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message="optimize payload too large for current context window",
                reason_code="payload_over_70pct",
                coverage_ratio=0.0,
                skipped_invalid_count=0,
                sealed_redirects=sealed_redirects,
            )

        try:
            alias_payload = eng.build_llm_view(target_bucket, payload)
            map_ver = eng.alias_map_version(target_bucket)
            eng.assert_alias_only_payload(target_bucket, alias_payload)
            llm_alias = await eng.pipeline.optimize(
                bucket_context=None,
                reason=reason,
                payload=alias_payload,
            )
            eng._audit_alias_llm_call(
                tool="optimize",
                bucket_id=target_bucket,
                map_version=map_ver,
                alias_input=alias_payload,
                alias_output=llm_alias,
            )
            llm_out = eng.resolve_llm_output(target_bucket, llm_alias, map_version=map_ver)
        except AliasPayloadError as exc:
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message=f"optimize alias failure: {exc}",
                reason_code="alias_failure",
                sealed_redirects=sealed_redirects,
            )

        eng._record_llm_usage()
        eng._record_llm_diag()
        llm_path_source = "LOCAL" if bool(eng.pipeline.last_diagnostics.get("degraded", False)) else "LLM"

        if bool(llm_out.get("skip_optimize", False)):
            skip_reason = str(llm_out.get("skip_reason", "")).strip() or "model judged current structure as reasonable"
            eng.storage.append_event(
                event_type="OPTIMIZE_SKIP",
                bucket_id=target_bucket,
                payload={
                    "request_id": eng._next_alias_request_id("optimize_skip"),
                    "reason": reason,
                    "message": skip_reason,
                    "result_source": llm_path_source,
                },
            )
            return OptimizeResult(
                success=True,
                bucket_id=target_bucket,
                message=f"optimize skipped: {skip_reason}",
                reason_code="skip_by_model",
                coverage_ratio=1.0,
                skipped_invalid_count=0,
                sealed_redirects=sealed_redirects,
            )

        prepared = self._prepare_plan(
            target_bucket=target_bucket,
            llm_out=llm_out,
            parent_keys=parent_keys,
            candidate_records=candidate_records,
            bucket_id_to_node_key=bucket_id_to_node_key,
            child_expansions=child_expansions,
        )
        duplicate_leaf_keys = [
            k
            for k, n in prepared.get("effective_key_counts", {}).items()
            if n > 1 and k in leaf_node_keys
        ]
        if duplicate_leaf_keys:
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message=f"duplicate leaf keys in optimize plan: {duplicate_leaf_keys[:5]}",
                reason_code="duplicate_leaf_keys",
                coverage_ratio=prepared["coverage_ratio"],
                skipped_invalid_count=prepared["skipped_invalid_count"],
                sealed_redirects=sealed_redirects,
            )

        leaf_check = self._validate_leaf_retention(
            leaf_node_keys=leaf_node_keys,
            leaf_bucket_keys=leaf_bucket_keys,
            mentioned_keys=set(prepared["mentioned_keys"]),
        )
        if not leaf_check["ok"]:
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message=str(leaf_check.get("message", "leaf retention check failed")),
                reason_code=str(leaf_check.get("reason_code", "leaf_retention_failed")),
                coverage_ratio=prepared["coverage_ratio"],
                skipped_invalid_count=prepared["skipped_invalid_count"],
                sealed_redirects=sealed_redirects,
            )

        if prepared["coverage_ratio"] < 0.60:
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message="optimize coverage too low",
                reason_code="coverage_too_low",
                coverage_ratio=prepared["coverage_ratio"],
                skipped_invalid_count=prepared["skipped_invalid_count"],
                sealed_redirects=sealed_redirects,
            )

        if prepared["parent_count"] > 800 or prepared["max_group_count"] > 800:
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message="too many elements in parent/group bucket",
                reason_code="elements_over_800",
                coverage_ratio=prepared["coverage_ratio"],
                skipped_invalid_count=prepared["skipped_invalid_count"],
                sealed_redirects=sealed_redirects,
            )

        bucket_plan = self._build_bucket_assignment_plan(target_bucket=target_bucket, prepared=prepared)
        pressure_check = self._validate_bucket_pressure(
            max_context_window=eng.max_context_window,
            candidate_records=candidate_records,
            bucket_plan=bucket_plan,
            target_bucket=target_bucket,
        )
        if not pressure_check["ok"]:
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message=str(pressure_check.get("message", "optimize pressure check failed")),
                reason_code=str(pressure_check.get("reason_code", "pressure_reject")),
                coverage_ratio=prepared["coverage_ratio"],
                skipped_invalid_count=prepared["skipped_invalid_count"],
                sealed_redirects=sealed_redirects,
            )

        req_id = eng._next_alias_request_id("optimize_apply")
        created_bucket_ids: list[str] = []
        moved_items = 0
        post_actions: list[dict[str, Any]] = []

        try:
            successor = eng._create_successor_bucket_shallow_unlocked(
                source_bucket_id=target_bucket,
                title=f"{info.title}_optimized",
                summary=(str(prepared.get("parent_summary", "")).strip() or info.summary),
            )

            group_bucket_mapping: dict[str, str] = {}
            for g in prepared["groups"]:
                gid = str(g["group_id"])
                existing_bucket_id = str(g.get("group_bucket_id", "")).strip()
                dst_bucket_id = ""
                if existing_bucket_id:
                    try:
                        rb = eng._resolve_bucket_id(existing_bucket_id)
                    except Exception:
                        rb = ""
                    if rb:
                        dst_info = eng.storage.get_bucket_info(rb)
                        if dst_info is not None and not dst_info.sealed:
                            dst_bucket_id = rb
                if not dst_bucket_id:
                    title = str(g.get("title", "")).strip() or f"optimized_group_{gid}"
                    summary = str(g.get("summary", "")).strip() or "optimized group"
                    content = str(g.get("content", "")).strip() or summary
                    created = eng._create_bucket_unlocked(
                        successor.bucket_id,
                        title=title[:80],
                        summary=summary[:140],
                        content=content[:1000],
                    )
                    created_bucket_ids.append(created.bucket_id)
                    dst_bucket_id = created.bucket_id
                group_bucket_mapping[gid] = dst_bucket_id

            assignment: dict[str, str] = {}
            for key in prepared["parent_flat_keys"]:
                assignment[key] = successor.bucket_id
            for g in prepared["groups"]:
                gid = str(g["group_id"])
                dst = group_bucket_mapping.get(gid, "")
                if not dst:
                    continue
                for key in g.get("member_keys", []):
                    if key in assignment and assignment[key] == successor.bucket_id:
                        continue
                    assignment[key] = dst

            retained_keys = set(prepared.get("retained_keys", []))
            omitted_keys = [k for k in candidate_records.keys() if k not in retained_keys]
            metadata_update = prepared.get("metadata_update", {})

            for key, dst_bucket in assignment.items():
                current = eng.storage.get_record(key)
                if current is None or current.gray:
                    continue
                dst_bucket_resolved = eng._resolve_bucket_id(dst_bucket)
                source_record = current
                row = metadata_update.get(key, {})
                if isinstance(row, dict) and row:
                    source_record = MemoryRecord.from_dict(current.to_dict())
                    if "relations" in row:
                        source_record.relations = normalize_relations(row.get("relations", {}))
                    if current.kind == BUCKET_KIND_BUCKET:
                        if "summary" in row:
                            source_record.summary = str(row.get("summary", "")).strip()[:140]
                        if "content" in row:
                            source_record.content = str(row.get("content", "")).strip()[:1000]
                eng._write_rebuilt_record_unlocked(
                    source_record=source_record,
                    dst_bucket_id=dst_bucket_resolved,
                    event="OPTIMIZE_REBUILD",
                    reason=reason,
                )
                if current.bucket_id != dst_bucket_resolved:
                    moved_items += 1

            for key in omitted_keys:
                current = eng.storage.get_record(key)
                if current is None or current.gray:
                    continue
                self._gray_out_record_unlocked(current=current, reason="optimize_plan_omitted")
                if current.kind == BUCKET_KIND_BUCKET:
                    self._seal_archive_child_bucket_unlocked(current)

            parent_summary = str(prepared.get("parent_summary", "")).strip()
            parent_content = str(prepared.get("parent_content", "")).strip()
            successor_info = eng.storage.get_bucket_info(successor.bucket_id)
            if successor_info is not None and (parent_summary or parent_content):
                if parent_summary:
                    successor_info.summary = parent_summary[:140]
                successor_info.summary_status = "ready"
                eng.storage.update_bucket_info(successor_info)
                eng._append_bucket_summary_update_event_unlocked(
                    info=successor_info,
                    summary=successor_info.summary,
                    content=(parent_content or successor_info.summary)[:1000],
                    reason=f"optimize:{reason}",
                )

            eng._seal_and_switch_bucket_unlocked(
                source_bucket_id=target_bucket,
                successor_bucket_id=successor.bucket_id,
                reason=reason,
            )

            eng.storage.append_alias_audit(
                {
                    "request_id": req_id,
                    "tool": "optimize_apply",
                    "bucket_id": target_bucket,
                    "successor_bucket_id": successor.bucket_id,
                    "map_version": map_ver,
                    "result_source": llm_path_source,
                    "coverage_ratio": prepared["coverage_ratio"],
                    "skipped_invalid_count": prepared["skipped_invalid_count"],
                    "created_bucket_ids": created_bucket_ids,
                    "moved_keys": moved_items,
                    "omitted_keys_count": len(omitted_keys),
                    "sealed_redirects": sealed_redirects,
                    "switched_at": utc_now_iso(),
                }
            )
            eng.storage.append_event(
                event_type="OPTIMIZE_APPLY",
                bucket_id=target_bucket,
                payload={
                    "request_id": req_id,
                    "reason": reason,
                    "result_source": llm_path_source,
                    "coverage_ratio": prepared["coverage_ratio"],
                    "skipped_invalid_count": prepared["skipped_invalid_count"],
                    "created_bucket_ids": created_bucket_ids,
                    "moved_items": moved_items,
                    "omitted_keys": len(omitted_keys),
                    "sealed_redirects": sealed_redirects,
                    "successor_bucket_id": successor.bucket_id,
                },
            )

            post_targets = [b for b in pressure_check["post_action_buckets"] if not str(b).startswith("__new_group__")]
            if post_targets and len(post_targets) < 3:
                for bid in post_targets:
                    try:
                        await eng._force_compress_unlocked(bucket_id=bid, reason="optimize_post_action")
                        await eng._auto_manage_bucket(bid)
                        post_actions.append({"bucket_id": bid, "action": "compress_split", "ok": True})
                    except Exception as exc:
                        post_actions.append({"bucket_id": bid, "action": "compress_split", "ok": False, "error": str(exc)})

            await eng._run_memory_gc()
            return OptimizeResult(
                success=True,
                bucket_id=successor.bucket_id,
                message="optimize applied via successor rebuild",
                reason_code="ok",
                coverage_ratio=prepared["coverage_ratio"],
                skipped_invalid_count=prepared["skipped_invalid_count"],
                created_buckets=created_bucket_ids,
                moved_items=moved_items,
                sealed_redirects=sealed_redirects,
                post_actions=post_actions,
            )
        except Exception as exc:
            return OptimizeResult(
                success=False,
                bucket_id=target_bucket,
                message=f"optimize failed: {exc}",
                reason_code="apply_failed",
                coverage_ratio=prepared.get("coverage_ratio", 0.0),
                skipped_invalid_count=prepared.get("skipped_invalid_count", 0),
                created_buckets=created_bucket_ids,
                moved_items=moved_items,
                sealed_redirects=sealed_redirects,
                post_actions=post_actions,
            )

    def _collect_candidates(
        self,
        target_bucket: str,
    ) -> tuple[
        dict[str, MemoryRecord],
        list[str],
        list[dict[str, Any]],
        dict[str, str],
        set[str],
        set[str],
        dict[str, str],
    ]:
        eng = self.runtime.engine
        candidate_records: dict[str, MemoryRecord] = {}
        parent_keys: list[str] = []
        child_expansions: list[dict[str, Any]] = []
        sealed_redirects: dict[str, str] = {}
        bucket_id_to_node_key: dict[str, str] = {}

        direct_records = eng.storage.list_bucket_records(target_bucket, include_gray=False)
        for rec in direct_records:
            if rec.key not in candidate_records:
                candidate_records[rec.key] = rec
            parent_keys.append(rec.key)
            if rec.kind == BUCKET_KIND_BUCKET and str(rec.child_bucket_id or "").strip():
                raw_child = str(rec.child_bucket_id).strip()
                try:
                    child_bucket = eng._resolve_bucket_id(raw_child)
                except Exception:
                    child_bucket = raw_child
                if child_bucket and raw_child and child_bucket != raw_child:
                    sealed_redirects[raw_child] = child_bucket
                if child_bucket:
                    bucket_id_to_node_key[child_bucket] = rec.key
                child_records = eng.storage.list_bucket_records(child_bucket, include_gray=False) if child_bucket else []
                expanded_keys: list[str] = []
                for c in child_records:
                    if c.key not in candidate_records:
                        candidate_records[c.key] = c
                    expanded_keys.append(c.key)
                    if c.kind == BUCKET_KIND_BUCKET and str(c.child_bucket_id or "").strip():
                        try:
                            grand_child = eng._resolve_bucket_id(str(c.child_bucket_id).strip())
                        except Exception:
                            grand_child = str(c.child_bucket_id).strip()
                        if grand_child:
                            bucket_id_to_node_key[grand_child] = c.key
                child_expansions.append(
                    {
                        "parent_node_key": rec.key,
                        "bucket_id": child_bucket,
                        "direct_keys": expanded_keys,
                    }
                )

        # Leaf semantics for optimize validation:
        # - Parent/root direct bucket nodes are internal trunks (not leaf buckets).
        # - Parent/root direct memory nodes are leaves.
        # - Child-layer direct items are all treated as leaves (memory or bucket),
        #   because optimize does not expand beyond this layer.
        parent_key_set = set(parent_keys)
        parent_bucket_keys = {
            rec.key
            for rec in direct_records
            if rec.kind == BUCKET_KIND_BUCKET
        }
        child_direct_keys: set[str] = set()
        for item in child_expansions:
            direct_keys = item.get("direct_keys", [])
            if not isinstance(direct_keys, list):
                continue
            for key in direct_keys:
                token = str(key).strip()
                if token:
                    child_direct_keys.add(token)

        leaf_node_keys: set[str] = set()
        leaf_bucket_keys: set[str] = set()
        for key in parent_key_set:
            if key in parent_bucket_keys:
                continue
            if key in candidate_records:
                leaf_node_keys.add(key)
                if candidate_records[key].kind == BUCKET_KIND_BUCKET:
                    leaf_bucket_keys.add(key)

        for key in child_direct_keys:
            rec = candidate_records.get(key)
            if rec is None:
                continue
            leaf_node_keys.add(key)
            if rec.kind == BUCKET_KIND_BUCKET:
                leaf_bucket_keys.add(key)

        return (
            candidate_records,
            parent_keys,
            child_expansions,
            sealed_redirects,
            leaf_node_keys,
            leaf_bucket_keys,
            bucket_id_to_node_key,
        )

    def _build_optimize_payload(
        self,
        *,
        target_bucket: str,
        info: BucketInfo,
        parent_keys: list[str],
        child_expansions: list[dict[str, Any]],
        candidate_records: dict[str, MemoryRecord],
        leaf_nodes: set[str],
        reason: str,
    ) -> dict[str, Any]:
        def _record_metadata(rec: MemoryRecord) -> dict[str, Any]:
            meta: dict[str, Any] = {
                "kind": rec.kind,
                "title": rec.title,
                "summary": rec.summary,
                "relations": rec.relations,
                "weight": rec.weight,
            }
            return meta

        expanded_by_parent: dict[str, list[str]] = {}
        for item in child_expansions:
            if not isinstance(item, dict):
                continue
            parent_raw = str(item.get("parent_node_key", "")).strip()
            direct_keys_raw = item.get("direct_keys", [])
            if not parent_raw or not isinstance(direct_keys_raw, list):
                continue
            expanded_by_parent[parent_raw] = [str(k).strip() for k in direct_keys_raw if str(k).strip()]

        root_children: dict[str, Any] = {}
        for key in parent_keys:
            rec = candidate_records.get(key)
            if rec is None:
                continue
            node_id = self._node_view_key(rec)
            child_nodes: dict[str, Any] = {}
            for child_key in expanded_by_parent.get(key, []):
                child_rec = candidate_records.get(child_key)
                if child_rec is None:
                    continue
                child_id = self._node_view_key(child_rec)
                child_nodes[child_id] = {
                    "metadata": _record_metadata(child_rec),
                    "children": {},
                }
            root_children[node_id] = {
                "metadata": _record_metadata(rec),
                "children": child_nodes,
            }

        return {
            "reason": reason,
            "tree": {
                "ROOT": {
                    "metadata": {
                        "kind": BUCKET_KIND_BUCKET,
                        "bucket_id": target_bucket,
                        "level": int(info.level),
                        "title": info.title,
                        "summary": info.summary,
                    },
                    "children": root_children,
                }
            },
            "constraints": {
                "max_levels": 2,
                "leaf_must_cover_all": True,
                "leaf_no_duplicates": True,
                "allow_skip": True,
                "prefer_parent_buckets": True,
                "prefer_group_memories": True,
                "soft_container_limit": 500,
            },
        }

    def _prepare_plan(
        self,
        *,
        target_bucket: str,
        llm_out: dict[str, Any],
        parent_keys: list[str],
        candidate_records: dict[str, MemoryRecord],
        bucket_id_to_node_key: dict[str, str],
        child_expansions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        eng = self.runtime.engine
        candidate_keys = set(candidate_records.keys())
        skipped_invalid_count = 0
        child_expand_map: dict[str, list[str]] = {}
        for item in child_expansions:
            if not isinstance(item, dict):
                continue
            p = str(item.get("parent_node_key", "")).strip()
            keys_raw = item.get("direct_keys", [])
            if not p or not isinstance(keys_raw, list):
                continue
            keys = [str(x).strip() for x in keys_raw if str(x).strip() in candidate_keys]
            child_expand_map[p] = keys

        def _parse_node_token(value: Any) -> str:
            token = str(value).strip()
            if not token:
                return ""
            if token in candidate_keys:
                return token
            node_key = str(bucket_id_to_node_key.get(token, "")).strip()
            if node_key and node_key in candidate_keys:
                return node_key
            try:
                resolved = str(eng.resolve_alias(target_bucket, token, None)).strip()
            except Exception:
                resolved = ""
            if resolved in candidate_keys:
                return resolved
            node_key = str(bucket_id_to_node_key.get(resolved, "")).strip()
            if node_key and node_key in candidate_keys:
                return node_key
            return ""

        def _parse_key_list(raw: Any) -> list[str]:
            out: list[str] = []
            if not isinstance(raw, list):
                return out
            for item in raw:
                node_key = _parse_node_token(item)
                if node_key:
                    out.append(node_key)
            return out

        def _expand_with_attached(keys: list[str]) -> list[str]:
            out: list[str] = []
            for key in keys:
                out.append(key)
                attached = child_expand_map.get(key, [])
                if attached:
                    out.extend(attached)
            return out

        def _unique_preserve(keys: list[str], *, deny: set[str] | None = None) -> list[str]:
            deny = deny or set()
            out: list[str] = []
            seen: set[str] = set()
            for key in keys:
                if key in deny or key in seen:
                    continue
                seen.add(key)
                out.append(key)
            return out

        parent_selected = _parse_key_list(llm_out.get("parent_flat_keys", []))

        groups: list[dict[str, Any]] = []
        groups_raw = llm_out.get("groups", [])
        if isinstance(groups_raw, list):
            idx = 0
            for item in groups_raw:
                if not isinstance(item, dict):
                    continue
                member_keys = _parse_key_list(item.get("members", []))
                if not member_keys:
                    continue
                idx += 1
                groups.append(
                    {
                        "group_id": f"g{idx}",
                        "group_bucket_id": str(item.get("group_bucket_id", "")).strip(),
                        "title": str(item.get("title", "")).strip(),
                        "summary": str(item.get("summary", "")).strip(),
                        "content": str(item.get("content", "")).strip(),
                        "member_keys": member_keys,
                    }
                )
        if not parent_selected and not groups:
            parent_selected = [k for k in parent_keys if k in candidate_keys]

        # Explicit plan is what we will actually apply (no implicit expansion here).
        explicit_assigned: set[str] = set()
        parent_flat_keys = _unique_preserve(parent_selected)
        explicit_assigned.update(parent_flat_keys)
        final_groups: list[dict[str, Any]] = []
        for g in groups:
            member_keys = _unique_preserve(g["member_keys"], deny=explicit_assigned)
            if not member_keys:
                continue
            row = dict(g)
            row["member_keys"] = member_keys
            final_groups.append(row)
            explicit_assigned.update(member_keys)

        # Fold meaningless singleton-bucket groups:
        # if a group only contains one bucket node, treat it as a flat parent item.
        singleton_bucket_to_parent: list[str] = []
        compact_groups: list[dict[str, Any]] = []
        for g in final_groups:
            member_keys = g.get("member_keys", [])
            if isinstance(member_keys, list) and len(member_keys) == 1:
                only_key = str(member_keys[0]).strip()
                rec = candidate_records.get(only_key)
                if rec is not None and rec.kind == BUCKET_KIND_BUCKET:
                    singleton_bucket_to_parent.append(only_key)
                    continue
            compact_groups.append(g)
        final_groups = compact_groups
        if singleton_bucket_to_parent:
            parent_flat_keys = _unique_preserve(parent_flat_keys + singleton_bucket_to_parent)

        # Recompute explicit assignment after singleton-group folding.
        explicit_assigned = set(parent_flat_keys)
        for g in final_groups:
            for key in g.get("member_keys", []):
                explicit_assigned.add(key)

        # Expanded plan is used only for validation semantics (leaf retention / duplicate checks).
        parent_expanded = _expand_with_attached(parent_flat_keys)
        groups_expanded: list[dict[str, Any]] = []
        for g in final_groups:
            expanded = _expand_with_attached(g["member_keys"])
            row = dict(g)
            row["member_keys"] = expanded
            groups_expanded.append(row)

        effective_counts: dict[str, int] = {}
        for key in parent_expanded:
            effective_counts[key] = int(effective_counts.get(key, 0)) + 1
        for g in groups_expanded:
            for key in g["member_keys"]:
                effective_counts[key] = int(effective_counts.get(key, 0)) + 1

        retained_keys: set[str] = set()
        retained_keys.update(_unique_preserve(parent_expanded))
        for g in groups_expanded:
            retained_keys.update(_unique_preserve(g["member_keys"]))

        retain_in_place_keys = sorted(retained_keys.difference(explicit_assigned))

        metadata_updates: dict[str, dict[str, Any]] = {}
        metadata_raw = llm_out.get("metadata_update", {})
        if isinstance(metadata_raw, dict):
            for raw_key, raw_row in metadata_raw.items():
                if not isinstance(raw_row, dict):
                    skipped_invalid_count += 1
                    continue
                node_key = _parse_node_token(raw_key)
                if not node_key:
                    skipped_invalid_count += 1
                    continue
                rec = candidate_records.get(node_key)
                if rec is None:
                    skipped_invalid_count += 1
                    continue
                clean: dict[str, Any] = {}
                if "relations" in raw_row:
                    clean["relations"] = normalize_relations(raw_row.get("relations", {}))
                if rec.kind == BUCKET_KIND_BUCKET:
                    if "summary" in raw_row:
                        clean["summary"] = str(raw_row.get("summary", "")).strip()[:140]
                    if "content" in raw_row:
                        clean["content"] = str(raw_row.get("content", "")).strip()[:1000]
                if clean:
                    metadata_updates[node_key] = clean
                else:
                    skipped_invalid_count += 1

        coverage_ratio = len(retained_keys) / max(1, len(candidate_keys))
        return {
            "parent_flat_keys": parent_flat_keys,
            "groups": final_groups,
            "mentioned_keys": sorted(retained_keys),
            "retained_keys": sorted(retained_keys),
            "retain_in_place_keys": retain_in_place_keys,
            "explicit_keys": sorted(explicit_assigned),
            "effective_key_counts": effective_counts,
            "coverage_ratio": _clamp_ratio(coverage_ratio),
            "skipped_invalid_count": skipped_invalid_count,
            "parent_count": len(parent_flat_keys),
            "max_group_count": max([len(g["member_keys"]) for g in final_groups], default=0),
            "parent_summary": str(llm_out.get("parent_summary", "")).strip(),
            "parent_content": str(llm_out.get("parent_content", "")).strip(),
            "metadata_update": metadata_updates,
        }

    def _build_bucket_assignment_plan(self, *, target_bucket: str, prepared: dict[str, Any]) -> dict[str, Any]:
        assignment: dict[str, str] = {}
        for key in prepared["parent_flat_keys"]:
            assignment[key] = target_bucket
        for g in prepared["groups"]:
            placeholder = f"__new_group__:{g['group_id']}"
            group_bucket_id = str(g.get("group_bucket_id", "")).strip()
            if group_bucket_id:
                placeholder = group_bucket_id
            for key in g["member_keys"]:
                if key in assignment and assignment[key] == target_bucket:
                    continue
                assignment[key] = placeholder
        return {
            "assignment": assignment,
            "retain_in_place_keys": list(prepared.get("retain_in_place_keys", [])),
        }

    def _validate_bucket_pressure(
        self,
        *,
        max_context_window: int,
        candidate_records: dict[str, MemoryRecord],
        bucket_plan: dict[str, Any],
        target_bucket: str,
    ) -> dict[str, Any]:
        eng = self.runtime.engine
        assignment: dict[str, str] = bucket_plan["assignment"]
        retain_in_place_keys = set(bucket_plan.get("retain_in_place_keys", []))

        touched_existing_buckets: set[str] = {target_bucket}
        for rec in candidate_records.values():
            touched_existing_buckets.add(rec.bucket_id)
        for dst in assignment.values():
            if dst and not dst.startswith("__new_group__"):
                try:
                    touched_existing_buckets.add(eng._resolve_bucket_id(dst))
                except Exception:
                    touched_existing_buckets.add(dst)

        sim: dict[str, list[MemoryRecord]] = {}
        for bid in touched_existing_buckets:
            try:
                rb = eng._resolve_bucket_id(bid)
            except Exception:
                rb = bid
            sim[rb] = eng.storage.list_bucket_records(rb, include_gray=False)

        candidate_keys = set(candidate_records.keys())
        for bid, items in list(sim.items()):
            sim[bid] = [r for r in items if r.key not in candidate_keys]

        for key, rec in candidate_records.items():
            if key in assignment:
                dst = assignment[key]
                if dst.startswith("__new_group__"):
                    dst_bucket = dst
                else:
                    try:
                        dst_bucket = eng._resolve_bucket_id(dst)
                    except Exception:
                        dst_bucket = dst
                sim.setdefault(dst_bucket, []).append(rec)
                continue
            if key in retain_in_place_keys:
                sim.setdefault(rec.bucket_id, []).append(rec)

        parent_count = len(sim.get(target_bucket, []))
        if parent_count > 800:
            return {"ok": False, "reason_code": "parent_elements_over_800", "message": "parent bucket elements exceed 800"}
        group_counts = [len(v) for k, v in sim.items() if k != target_bucket]
        if any(c > 800 for c in group_counts):
            return {"ok": False, "reason_code": "group_elements_over_800", "message": "group bucket elements exceed 800"}

        mid_buckets: list[str] = []
        max_window = max(1, int(max_context_window))
        for bid, records in sim.items():
            est = self._estimate_records_tokens(records)
            ratio = est / float(max_window)
            if ratio > 0.80:
                return {"ok": False, "reason_code": "bucket_over_80pct", "message": f"bucket pressure too high: {bid}"}
            if 0.70 < ratio < 0.80:
                mid_buckets.append(bid)
        if len(mid_buckets) >= 3:
            return {"ok": False, "reason_code": "mid_pressure_too_many", "message": "too many buckets in 70%-80% range"}
        return {"ok": True, "post_action_buckets": mid_buckets}

    def _is_leaf_bucket_node(self, rec: MemoryRecord) -> bool:
        eng = self.runtime.engine
        child_raw = str(rec.child_bucket_id or "").strip()
        if not child_raw:
            return True
        try:
            child = eng._resolve_bucket_id(child_raw)
        except Exception:
            child = child_raw
        if not child:
            return True
        info = eng.storage.get_bucket_info(child)
        if info is None:
            return True
        return len(info.children) == 0

    def _validate_leaf_retention(
        self,
        *,
        leaf_node_keys: set[str],
        leaf_bucket_keys: set[str],
        mentioned_keys: set[str],
    ) -> dict[str, Any]:
        eng = self.runtime.engine
        loss_threshold = max(0.0, min(1.0, float(getattr(eng, "_optimize_leaf_loss_threshold", 0.03))))
        if not leaf_node_keys:
            return {"ok": True}
        missing_leaf_nodes = {k for k in leaf_node_keys if k not in mentioned_keys}
        if not missing_leaf_nodes:
            return {"ok": True}
        missing_leaf_buckets = {k for k in leaf_bucket_keys if k not in mentioned_keys}
        if missing_leaf_buckets:
            return {
                "ok": False,
                "reason_code": "leaf_bucket_missing",
                "message": f"leaf bucket missing: {sorted(missing_leaf_buckets)[:3]}",
            }
        ratio = len(missing_leaf_nodes) / max(1, len(leaf_node_keys))
        if ratio > loss_threshold:
            return {
                "ok": False,
                "reason_code": "leaf_loss_over_threshold",
                "message": f"leaf node loss ratio too high: {ratio:.2%} > {loss_threshold:.0%}",
            }
        return {"ok": True}

    def _gray_out_record_unlocked(self, *, current: MemoryRecord, reason: str) -> None:
        eng = self.runtime.engine
        rel = normalize_relations(current.relations)
        eng._append_relation_once(
            rel["lifecycle_links"],
            target=current.revision_id,
            rel_type="tombstones",
            score=1.0,
            note=reason,
        )
        out_rec = MemoryRecord(
            key=current.key,
            revision_id=eng.storage.generate_revision_id(),
            kind=current.kind,
            bucket_id=current.bucket_id,
            title=current.title,
            summary=current.summary,
            content=current.content,
            weight=current.weight,
            event="GRAY_SET",
            gray=True,
            relations=rel,
            evidence_ref=current.evidence_ref,
            expires_at=current.expires_at,
            source_hash=current.source_hash,
            child_bucket_id=current.child_bucket_id,
        )
        eng.storage.write_memory_record(out_rec)
        eng._append_context_event(
            bucket_id=current.bucket_id,
            event_type="GRAY_SET",
            record=out_rec,
            payload={"from_revision": current.revision_id, "reason": reason},
        )

    def _seal_archive_child_bucket_unlocked(self, bucket_node: MemoryRecord) -> None:
        eng = self.runtime.engine
        child_raw = str(bucket_node.child_bucket_id or "").strip()
        if not child_raw:
            return
        try:
            child_bucket = eng._resolve_bucket_id(child_raw)
        except Exception:
            child_bucket = child_raw
        if not child_bucket:
            return
        info = eng.storage.get_bucket_info(child_bucket)
        if info is None:
            return
        info.sealed = True
        info.archived = True
        info.updated_at = utc_now_iso()
        eng.storage.update_bucket_info(info)
        try:
            eng.freeze_alias_map(child_bucket)
        except Exception:
            pass

    @staticmethod
    def _estimate_records_tokens(records: list[MemoryRecord]) -> int:
        total_chars = 0
        for rec in records:
            total_chars += len(rec.title) + len(rec.summary) + len(rec.content)
            for rel_name, rel_items in rec.relations.items():
                total_chars += len(rel_name)
                for item in rel_items:
                    total_chars += len(json.dumps(item, ensure_ascii=False))
        return max(1, total_chars // 3)
