from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from context_memory.LLM_connect import Context, TextPrompt

from .models import BucketInfo, MemoryRecord, normalize_relations, utc_now_iso


class MemoryStorageV3:
    def __init__(self, root_dir: str | Path, *, prompt_version: str = "v3", evidence_versions: int = 5) -> None:
        self.root_dir = Path(root_dir)
        self.memories_dir = self.root_dir / "memories"
        self.evidence_dir = self.root_dir / "evidence"
        self.buckets_dir = self.root_dir / "buckets"
        self.snapshots_dir = self.root_dir / "snapshots"
        self.index_dir = self.root_dir / "index"
        self.jobs_dir = self.index_dir / "jobs"
        self.events_file = self.index_dir / "events.ndjson"
        self.alias_audit_file = self.index_dir / "alias_audit.ndjson"
        self.state_file = self.index_dir / "state.json"
        self.meta_file = self.index_dir / "meta.json"
        self.cache_file = self.index_dir / "query_cache.json"
        self.bucket_tree_file = self.index_dir / "bucket_tree.json"
        self._prompt_version = prompt_version
        self._evidence_versions = max(1, int(evidence_versions))
        self._alias_map_cache: dict[str, dict[str, Any]] = {}
        self._alias_map_dirty: set[str] = set()
        self._alias_session_depth: int = 0
        self._ensure_layout()

    def _ensure_layout(self) -> None:
        for p in (self.memories_dir, self.evidence_dir, self.buckets_dir, self.snapshots_dir, self.index_dir, self.jobs_dir):
            p.mkdir(parents=True, exist_ok=True)
        if not self.events_file.exists():
            self.events_file.write_text("", encoding="utf-8")
        if not self.alias_audit_file.exists():
            self.alias_audit_file.write_text("", encoding="utf-8")
        if not self.state_file.exists():
            self._atomic_save_json({"keys": {}, "revision_total": 0}, self.state_file)
        if not self.meta_file.exists():
            self._atomic_save_json(
                {
                    "dirty": False,
                    "context_version": 0,
                    "prompt_version": self._prompt_version,
                    "updated_at": utc_now_iso(),
                    "last_snapshot": "",
                    "llm_calls_total": 0,
                    "llm_input_tokens_total": 0,
                    "llm_output_tokens_total": 0,
                    "llm_cached_input_tokens_total": 0,
                    "degraded_query_total": 0,
                    "llm_parse_fail_total": 0,
                    "llm_precheck_fail_total": 0,
                    "clean_reject_total": 0,
                    "clean_fallback_total": 0,
                    "ingest_blocked_by_clean_total": 0,
                    "context_overflow_total": 0,
                    "overflow_query_total": 0,
                    "overflow_ingest_total": 0,
                    "overflow_compress_total": 0,
                    "file_import_reject_total": 0,
                    "auto_split_guard_hit_total": 0,
                    "auto_split_cooldown_skip_total": 0,
                    "auto_split_no_progress_total": 0,
                    "split_plan_warn_total": 0,
                    "last_auto_split_at": "",
                    "last_split_source_bucket_id": "",
                    "last_split_successor_bucket_id": "",
                    "auto_split_last_at_by_bucket": {},
                    "bucket_versions": {},
                    "real_key_leak_count": 0,
                    "alias_resolve_fail_count": 0,
                    "unknown_alias_count": 0,
                    "query_alias_miss_build_total": 0,
                    "query_alias_miss_resolve_total": 0,
                    "query_side_effect_drop_total": 0,
                },
                self.meta_file,
            )
        if not self.cache_file.exists():
            self._atomic_save_json({}, self.cache_file)
        if not self.bucket_tree_file.exists():
            root_bucket_id = self.generate_bucket_id()
            root_info = BucketInfo(
                bucket_id=root_bucket_id,
                parent_bucket_id=None,
                level=1,
                title="ROOT",
                summary="root bucket",
                node_key="",
            )
            tree = {
                "root_bucket_id": root_bucket_id,
                "active_bucket_id": root_bucket_id,
                "buckets": {root_bucket_id: root_info.to_dict()},
                "updated_at": utc_now_iso(),
            }
            self._atomic_save_json(tree, self.bucket_tree_file)
            self.ensure_bucket_files(root_bucket_id)

    @staticmethod
    def _atomic_save_json(content: object, target_path: Path) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = target_path.with_name(f"{target_path.name}.tmp")
        tmp.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(target_path)

    @staticmethod
    def _load_json(path: Path, default: dict) -> dict:
        if not path.exists():
            return default.copy()
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return default.copy()
        if not isinstance(loaded, dict):
            return default.copy()
        return loaded

    def _to_relative_root_path(self, path: Path) -> str:
        try:
            return str(path.resolve().relative_to(self.root_dir.resolve()))
        except Exception:
            return str(path)

    def _resolve_root_path(self, path_text: str) -> Path:
        p = Path(str(path_text))
        if p.is_absolute():
            return p
        return self.root_dir / p

    @staticmethod
    def generate_key() -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"mem_{stamp}_{uuid4().hex}"

    @staticmethod
    def generate_revision_id() -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"rev_{stamp}_{uuid4().hex}"

    @staticmethod
    def generate_bucket_id() -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"bucket_{stamp}_{uuid4().hex}"

    def load_state(self) -> dict:
        return self._load_json(self.state_file, {"keys": {}, "revision_total": 0})

    def save_state(self, state: dict) -> None:
        self._atomic_save_json(state, self.state_file)

    def load_meta(self) -> dict:
        return self._load_json(
            self.meta_file,
            {
                "dirty": False,
                "context_version": 0,
                "prompt_version": self._prompt_version,
                "updated_at": utc_now_iso(),
                "last_snapshot": "",
                "llm_calls_total": 0,
                "llm_input_tokens_total": 0,
                "llm_output_tokens_total": 0,
                "llm_cached_input_tokens_total": 0,
                "degraded_query_total": 0,
                "llm_parse_fail_total": 0,
                "llm_precheck_fail_total": 0,
                "clean_reject_total": 0,
                "clean_fallback_total": 0,
                "ingest_blocked_by_clean_total": 0,
                "context_overflow_total": 0,
                "overflow_query_total": 0,
                "overflow_ingest_total": 0,
                "overflow_compress_total": 0,
                "file_import_reject_total": 0,
                "auto_split_guard_hit_total": 0,
                "auto_split_cooldown_skip_total": 0,
                "auto_split_no_progress_total": 0,
                "split_plan_warn_total": 0,
                "last_auto_split_at": "",
                "last_split_source_bucket_id": "",
                "last_split_successor_bucket_id": "",
                "auto_split_last_at_by_bucket": {},
                "bucket_versions": {},
                "real_key_leak_count": 0,
                "alias_resolve_fail_count": 0,
                "unknown_alias_count": 0,
                "query_alias_miss_build_total": 0,
                "query_alias_miss_resolve_total": 0,
                "query_side_effect_drop_total": 0,
            },
        )

    def save_meta(self, meta: dict) -> None:
        self._atomic_save_json(meta, self.meta_file)

    def load_cache(self) -> dict:
        return self._load_json(self.cache_file, {})

    def save_cache(self, cache: dict) -> None:
        self._atomic_save_json(cache, self.cache_file)

    def load_bucket_tree(self) -> dict:
        default = {"root_bucket_id": "", "active_bucket_id": "", "buckets": {}, "updated_at": utc_now_iso()}
        return self._load_json(self.bucket_tree_file, default)

    def save_bucket_tree(self, tree: dict) -> None:
        tree["updated_at"] = utc_now_iso()
        self._atomic_save_json(tree, self.bucket_tree_file)

    def get_root_bucket_id(self) -> str:
        tree = self.load_bucket_tree()
        root_id = str(tree.get("root_bucket_id", "")).strip()
        if not root_id:
            raise RuntimeError("bucket tree missing root_bucket_id")
        return root_id

    def get_active_bucket_id(self) -> str:
        tree = self.load_bucket_tree()
        active = str(tree.get("active_bucket_id", "")).strip()
        if active:
            return active
        return self.get_root_bucket_id()

    def set_active_bucket_id(self, bucket_id: str) -> None:
        tree = self.load_bucket_tree()
        tree["active_bucket_id"] = bucket_id
        self.save_bucket_tree(tree)

    def set_root_bucket_id(self, bucket_id: str) -> None:
        tree = self.load_bucket_tree()
        tree["root_bucket_id"] = bucket_id
        self.save_bucket_tree(tree)

    def set_root_and_active_bucket_id(self, bucket_id: str) -> None:
        tree = self.load_bucket_tree()
        tree["root_bucket_id"] = bucket_id
        tree["active_bucket_id"] = bucket_id
        self.save_bucket_tree(tree)

    def list_buckets(self) -> list[BucketInfo]:
        tree = self.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            return []
        out: list[BucketInfo] = []
        for _, raw in buckets.items():
            if isinstance(raw, dict):
                out.append(BucketInfo.from_dict(raw))
        return out

    def get_bucket_info(self, bucket_id: str) -> BucketInfo | None:
        tree = self.load_bucket_tree()
        raw = tree.get("buckets", {}).get(bucket_id)
        if not isinstance(raw, dict):
            return None
        return BucketInfo.from_dict(raw)

    def ensure_bucket_files(self, bucket_id: str) -> None:
        bdir = self.buckets_dir / bucket_id
        bdir.mkdir(parents=True, exist_ok=True)
        context_path = bdir / "context.json"
        events_path = bdir / "events.ndjson"
        alias_map_path = bdir / "alias_map.json"
        if not context_path.exists():
            context_path.write_text(json.dumps(self._empty_context_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        if not events_path.exists():
            events_path.write_text("", encoding="utf-8")
        if not alias_map_path.exists():
            self._atomic_save_json(self._default_alias_map(bucket_id), alias_map_path)

    def _bucket_alias_map_path(self, bucket_id: str) -> Path:
        self.ensure_bucket_files(bucket_id)
        return self.buckets_dir / bucket_id / "alias_map.json"

    @staticmethod
    def _default_alias_map(bucket_id: str) -> dict[str, Any]:
        return {
            "bucket_id": bucket_id,
            "map_version": 1,
            "sealed": False,
            "real_to_alias": {},
            "alias_to_real": {},
            "counters": {
                "memory": 0,
                "bucket": 0,
                "revision": 0,
                "ref": 0,
            },
            "updated_at": utc_now_iso(),
        }

    def load_alias_map(self, bucket_id: str) -> dict[str, Any]:
        cached = self._alias_map_cache.get(bucket_id)
        if isinstance(cached, dict):
            return cached
        path = self._bucket_alias_map_path(bucket_id)
        loaded = self._load_json(path, self._default_alias_map(bucket_id))
        if str(loaded.get("bucket_id", "")).strip() != bucket_id:
            loaded["bucket_id"] = bucket_id
        if not isinstance(loaded.get("real_to_alias"), dict):
            loaded["real_to_alias"] = {}
        if not isinstance(loaded.get("alias_to_real"), dict):
            loaded["alias_to_real"] = {}
        counters = loaded.get("counters", {})
        if not isinstance(counters, dict):
            counters = {}
        for key in ("memory", "bucket", "revision", "ref"):
            counters[key] = max(0, int(counters.get(key, 0)))
        loaded["counters"] = counters
        loaded["map_version"] = max(1, int(loaded.get("map_version", 1)))
        loaded["sealed"] = bool(loaded.get("sealed", False))
        self._alias_map_cache[bucket_id] = loaded
        return loaded

    def save_alias_map(self, bucket_id: str, payload: dict[str, Any]) -> None:
        body = dict(payload)
        body["bucket_id"] = bucket_id
        body["updated_at"] = utc_now_iso()
        self._alias_map_cache[bucket_id] = body
        self._alias_map_dirty.add(bucket_id)
        if self._alias_session_depth <= 0:
            self.flush_alias_maps(bucket_id=bucket_id)

    def begin_alias_session(self) -> None:
        self._alias_session_depth += 1

    def end_alias_session(self, *, flush: bool = True) -> None:
        if self._alias_session_depth > 0:
            self._alias_session_depth -= 1
        if flush and self._alias_session_depth <= 0:
            self.flush_alias_maps()

    def flush_alias_maps(self, bucket_id: str | None = None) -> int:
        targets: list[str]
        if bucket_id:
            targets = [bucket_id] if bucket_id in self._alias_map_dirty else []
        else:
            targets = sorted(self._alias_map_dirty)
        saved = 0
        for bid in targets:
            payload = self._alias_map_cache.get(bid)
            if not isinstance(payload, dict):
                self._alias_map_dirty.discard(bid)
                continue
            body = dict(payload)
            body["bucket_id"] = bid
            body["updated_at"] = utc_now_iso()
            self._atomic_save_json(body, self._bucket_alias_map_path(bid))
            self._alias_map_cache[bid] = body
            self._alias_map_dirty.discard(bid)
            saved += 1
        return saved

    def alias_map_version(self, bucket_id: str) -> int:
        amap = self.load_alias_map(bucket_id)
        return max(1, int(amap.get("map_version", 1)))

    @staticmethod
    def _is_valid_real_key_for_type(real_key: str, key_type: str) -> bool:
        key = str(real_key or "").strip()
        if not key:
            return False
        t = str(key_type or "").strip().lower()
        if t == "memory":
            return bool(re.match(r"^mem_[0-9]{14}_[0-9a-f]{32}$", key))
        if t == "bucket":
            return bool(re.match(r"^bucket_[0-9]{14}_[0-9a-f]{32}$", key))
        if t == "revision":
            return bool(re.match(r"^rev_[0-9]{14}_[0-9a-f]{32}$", key))
        if t == "ref":
            return True
        return False

    def _next_alias(self, amap: dict[str, Any], key_type: str) -> str:
        counters = amap.get("counters", {})
        if not isinstance(counters, dict):
            counters = {}
        current = max(0, int(counters.get(key_type, 0))) + 1
        counters[key_type] = current
        amap["counters"] = counters
        return f"{key_type}_{current}"

    def get_or_create_alias(self, bucket_id: str, real_key: str, key_type: str) -> str:
        k = str(real_key or "").strip()
        t = str(key_type or "").strip().lower()
        if not self._is_valid_real_key_for_type(k, t):
            raise ValueError(f"invalid real key for type={t}: {k}")
        amap = self.load_alias_map(bucket_id)
        real_to_alias = amap.get("real_to_alias", {})
        if not isinstance(real_to_alias, dict):
            real_to_alias = {}
        typed_key = f"{t}:{k}"
        existing = str(real_to_alias.get(typed_key, "")).strip()
        if existing:
            return existing
        if bool(amap.get("sealed", False)):
            info = self.get_bucket_info(bucket_id)
            successor = str(info.sealed_to).strip() if info is not None and info.sealed else ""
            if successor and successor != bucket_id:
                return self.get_or_create_alias(successor, k, t)
            raise RuntimeError(f"alias map sealed; cannot allocate new alias in bucket={bucket_id}")

        alias_to_real = amap.get("alias_to_real", {})
        if not isinstance(real_to_alias, dict):
            real_to_alias = {}
        if not isinstance(alias_to_real, dict):
            alias_to_real = {}
        alias = self._next_alias(amap, t)
        real_to_alias[typed_key] = alias
        alias_to_real[alias] = {"key_type": t, "real_key": k}
        amap["real_to_alias"] = real_to_alias
        amap["alias_to_real"] = alias_to_real
        amap["map_version"] = int(amap.get("map_version", 1)) + 1
        self.save_alias_map(bucket_id, amap)
        return alias

    def find_alias(self, bucket_id: str, real_key: str, key_type: str) -> str | None:
        k = str(real_key or "").strip()
        t = str(key_type or "").strip().lower()
        if not self._is_valid_real_key_for_type(k, t):
            return None
        amap = self.load_alias_map(bucket_id)
        real_to_alias = amap.get("real_to_alias", {})
        if not isinstance(real_to_alias, dict):
            return None
        token = str(real_to_alias.get(f"{t}:{k}", "")).strip()
        return token or None

    def resolve_alias(self, bucket_id: str, alias: str, expected_type: str | None = None) -> str:
        token = str(alias or "").strip()
        if not token:
            raise ValueError("alias is empty")
        amap = self.load_alias_map(bucket_id)
        alias_to_real = amap.get("alias_to_real", {})
        if not isinstance(alias_to_real, dict):
            alias_to_real = {}
        raw = alias_to_real.get(token)
        if not isinstance(raw, dict):
            raise KeyError(f"unknown alias={token} in bucket={bucket_id}")
        key_type = str(raw.get("key_type", "")).strip().lower()
        real_key = str(raw.get("real_key", "")).strip()
        if expected_type:
            expect = str(expected_type).strip().lower()
            if expect and key_type != expect:
                raise TypeError(f"alias type mismatch: alias={token}, expected={expect}, got={key_type}")
        if not self._is_valid_real_key_for_type(real_key, key_type):
            raise ValueError(f"invalid mapped real key for alias={token}")
        return real_key

    def freeze_alias_map(self, bucket_id: str) -> None:
        amap = self.load_alias_map(bucket_id)
        if bool(amap.get("sealed", False)):
            return
        amap["sealed"] = True
        amap["map_version"] = int(amap.get("map_version", 1)) + 1
        self.save_alias_map(bucket_id, amap)

    def assert_bucket_writable(self, bucket_id: str) -> None:
        info = self.get_bucket_info(bucket_id)
        if info is not None and bool(info.sealed):
            raise RuntimeError(f"bucket is sealed and read-only: {bucket_id}")

    def create_bucket(
        self,
        *,
        parent_bucket_id: str | None,
        level: int,
        title: str,
        summary: str,
        node_key: str,
        summary_status: str = "ready",
        summary_locked: bool = False,
    ) -> BucketInfo:
        bucket_id = self.generate_bucket_id()
        self.ensure_bucket_files(bucket_id)

        tree = self.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            buckets = {}

        info = BucketInfo(
            bucket_id=bucket_id,
            parent_bucket_id=parent_bucket_id,
            level=level,
            title=title,
            summary=summary,
            node_key=node_key,
            summary_status=summary_status,
            summary_locked=summary_locked,
        )
        buckets[bucket_id] = info.to_dict()

        if parent_bucket_id:
            parent_raw = buckets.get(parent_bucket_id)
            if isinstance(parent_raw, dict):
                parent = BucketInfo.from_dict(parent_raw)
                if bucket_id not in parent.children:
                    parent.children.append(bucket_id)
                parent.updated_at = utc_now_iso()
                buckets[parent_bucket_id] = parent.to_dict()

        tree["buckets"] = buckets
        self.save_bucket_tree(tree)
        self.mark_bucket_dirty(bucket_id)
        return info

    def update_bucket_info(self, info: BucketInfo) -> None:
        tree = self.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            buckets = {}
        info.updated_at = utc_now_iso()
        buckets[info.bucket_id] = info.to_dict()
        tree["buckets"] = buckets
        self.save_bucket_tree(tree)

    def remove_child_link(self, *, parent_bucket_id: str, child_bucket_id: str) -> None:
        tree = self.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            return
        parent_raw = buckets.get(parent_bucket_id)
        if not isinstance(parent_raw, dict):
            return
        parent = BucketInfo.from_dict(parent_raw)
        parent.children = [c for c in parent.children if c != child_bucket_id]
        buckets[parent_bucket_id] = parent.to_dict()
        tree["buckets"] = buckets
        self.save_bucket_tree(tree)

    def add_child_link(self, *, parent_bucket_id: str, child_bucket_id: str) -> None:
        tree = self.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            return
        parent_raw = buckets.get(parent_bucket_id)
        if not isinstance(parent_raw, dict):
            return
        parent = BucketInfo.from_dict(parent_raw)
        if child_bucket_id not in parent.children:
            parent.children.append(child_bucket_id)
        buckets[parent_bucket_id] = parent.to_dict()
        tree["buckets"] = buckets
        self.save_bucket_tree(tree)

    def reparent_bucket(self, *, bucket_id: str, new_parent_bucket_id: str | None) -> None:
        tree = self.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            return
        raw = buckets.get(bucket_id)
        if not isinstance(raw, dict):
            return
        child = BucketInfo.from_dict(raw)
        old_parent = child.parent_bucket_id
        child.parent_bucket_id = new_parent_bucket_id
        child.updated_at = utc_now_iso()
        buckets[bucket_id] = child.to_dict()
        if old_parent:
            old_raw = buckets.get(old_parent)
            if isinstance(old_raw, dict):
                p = BucketInfo.from_dict(old_raw)
                p.children = [c for c in p.children if c != bucket_id]
                p.updated_at = utc_now_iso()
                buckets[old_parent] = p.to_dict()
        if new_parent_bucket_id:
            new_raw = buckets.get(new_parent_bucket_id)
            if isinstance(new_raw, dict):
                p2 = BucketInfo.from_dict(new_raw)
                if bucket_id not in p2.children:
                    p2.children.append(bucket_id)
                p2.updated_at = utc_now_iso()
                buckets[new_parent_bucket_id] = p2.to_dict()
        tree["buckets"] = buckets
        self.save_bucket_tree(tree)

    def _empty_context_dict(self) -> dict[str, Any]:
        return Context().to_dict()

    def load_bucket_context(self, bucket_id: str):
        self.ensure_bucket_files(bucket_id)
        path = self.buckets_dir / bucket_id / "context.json"
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            raw = self._empty_context_dict()
        try:
            return Context.from_dict(raw, {}) if isinstance(raw, dict) else Context()
        except Exception:
            return Context()

    def save_bucket_context(self, bucket_id: str, context: Any) -> None:
        self.ensure_bucket_files(bucket_id)
        path = self.buckets_dir / bucket_id / "context.json"
        try:
            payload = context.to_dict() if context is not None else self._empty_context_dict()
        except Exception:
            payload = self._empty_context_dict()
        self._atomic_save_json(payload, path)

    @staticmethod
    def serialize_bucket_event(event: dict[str, Any]) -> str:
        return f"[MEM_EVENT]{json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(',', ':'))}"

    def append_bucket_event(self, bucket_id: str, event: dict[str, Any]) -> None:
        self.assert_bucket_writable(bucket_id)
        # Keep alias mapping durable before context append to avoid dangling alias references.
        self.flush_alias_maps(bucket_id=bucket_id)
        self.ensure_bucket_files(bucket_id)
        ctx = self.load_bucket_context(bucket_id)
        ctx.append(TextPrompt("user", self.serialize_bucket_event(event)))
        self.save_bucket_context(bucket_id, ctx)
        with (self.buckets_dir / bucket_id / "events.ndjson").open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True))
            f.write("\n")
        self.mark_bucket_dirty(bucket_id)

    def write_memory_record(self, record: MemoryRecord) -> Path:
        self.assert_bucket_writable(record.bucket_id)
        key_dir = self.memories_dir / record.key
        key_dir.mkdir(parents=True, exist_ok=True)
        save_path = key_dir / f"{record.revision_id}.json"
        save_path.write_text(json.dumps(record.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        relative_save_path = self._to_relative_root_path(save_path)

        state = self.load_state()
        keys = state.setdefault("keys", {})
        node = keys.get(record.key, {})
        if not isinstance(node, dict):
            node = {}

        evidence_history = node.get("evidence_history", [])
        if not isinstance(evidence_history, list):
            evidence_history = []
        if record.evidence_ref and record.evidence_ref not in evidence_history:
            evidence_history.append(record.evidence_ref)

        node.update(
            {
                "latest_revision": record.revision_id,
                "latest_path": relative_save_path,
                "bucket_id": record.bucket_id,
                "kind": record.kind,
                "child_bucket_id": record.child_bucket_id,
                "gray": bool(record.gray),
                "expires_at": record.expires_at,
                "updated_at": utc_now_iso(),
                "created_at": str(node.get("created_at", record.created_at)),
                "revision_count": int(node.get("revision_count", 0)) + 1,
                "latest_evidence_ref": record.evidence_ref,
                "evidence_history": evidence_history[-self._evidence_versions :],
                "query_hits": int(node.get("query_hits", 0)),
                "last_recalled_at": str(node.get("last_recalled_at", "")),
                "last_compress_penalty_at": str(node.get("last_compress_penalty_at", "")),
            }
        )
        keys[record.key] = node
        state["revision_total"] = int(state.get("revision_total", 0)) + 1
        self.save_state(state)
        self.mark_bucket_dirty(record.bucket_id)
        return save_path

    def get_record(self, key: str, revision_id: str | None = None) -> MemoryRecord | None:
        if revision_id:
            path = self.memories_dir / key / f"{revision_id}.json"
            if not path.exists():
                return None
            return self._json_to_memory_record(path)

        state = self.load_state()
        node = state.get("keys", {}).get(key)
        if not isinstance(node, dict):
            return None
        path_text = node.get("latest_path")
        if not isinstance(path_text, str):
            return None
        return self._json_to_memory_record(self._resolve_root_path(path_text))

    def list_latest_records(self, *, include_gray: bool = True) -> list[MemoryRecord]:
        state = self.load_state()
        out: list[MemoryRecord] = []
        for _, node in state.get("keys", {}).items():
            if not isinstance(node, dict):
                continue
            if not include_gray and bool(node.get("gray", False)):
                continue
            path_text = node.get("latest_path")
            if not isinstance(path_text, str):
                continue
            rec = self._json_to_memory_record(self._resolve_root_path(path_text))
            if rec is None:
                continue
            out.append(rec)
        return out

    def list_bucket_records(self, bucket_id: str, *, include_gray: bool = False) -> list[MemoryRecord]:
        out: list[MemoryRecord] = []
        for rec in self.list_latest_records(include_gray=True):
            if rec.bucket_id != bucket_id:
                continue
            if not include_gray and rec.gray:
                continue
            out.append(rec)
        return out

    def record_recall(self, key: str) -> None:
        state = self.load_state()
        node = state.get("keys", {}).get(key)
        if not isinstance(node, dict):
            return
        node["query_hits"] = int(node.get("query_hits", 0)) + 1
        node["last_recalled_at"] = utc_now_iso()
        node["updated_at"] = utc_now_iso()
        self.save_state(state)

    def set_last_compress_penalty(self, key: str) -> None:
        state = self.load_state()
        node = state.get("keys", {}).get(key)
        if not isinstance(node, dict):
            return
        node["last_compress_penalty_at"] = utc_now_iso()
        node["updated_at"] = utc_now_iso()
        self.save_state(state)

    def get_key_node(self, key: str) -> dict[str, Any] | None:
        state = self.load_state()
        node = state.get("keys", {}).get(key)
        return node if isinstance(node, dict) else None

    def copy_evidence(self, evidence_path: str | Path, *, key: str) -> str:
        source = Path(evidence_path)
        if not source.exists() or not source.is_file():
            return ""
        ext = source.suffix or ".txt"
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        evidence_id = f"evi_{stamp}_{uuid4().hex}"
        key_dir = self.evidence_dir / key
        key_dir.mkdir(parents=True, exist_ok=True)
        target = key_dir / f"{evidence_id}{ext}"
        shutil.copy2(source, target)
        evidence_ref = f"{key}/{target.name}"

        state = self.load_state()
        keys = state.setdefault("keys", {})
        node = keys.get(key, {})
        if not isinstance(node, dict):
            node = {}
        history = node.get("evidence_history", [])
        if not isinstance(history, list):
            history = []
        history.append(evidence_ref)

        to_delete = history[:-self._evidence_versions]
        kept = history[-self._evidence_versions :]
        for old_ref in to_delete:
            self._delete_evidence_ref(old_ref)

        node["evidence_history"] = kept
        node["latest_evidence_ref"] = evidence_ref
        node["updated_at"] = utc_now_iso()
        keys[key] = node
        self.save_state(state)
        return evidence_ref

    def _delete_evidence_ref(self, evidence_ref: str) -> None:
        if not evidence_ref:
            return
        target = self.evidence_dir / evidence_ref
        if target.exists() and target.is_file():
            target.unlink(missing_ok=True)
        parent = target.parent
        if parent.exists() and parent.is_dir() and parent != self.evidence_dir:
            if not any(parent.iterdir()):
                parent.rmdir()

    def purge_evidence_for_key(self, key: str) -> None:
        key_dir = self.evidence_dir / key
        if key_dir.exists() and key_dir.is_dir():
            shutil.rmtree(key_dir, ignore_errors=True)
        state = self.load_state()
        node = state.get("keys", {}).get(key)
        if isinstance(node, dict):
            node["evidence_history"] = []
            node["latest_evidence_ref"] = ""
            node["updated_at"] = utc_now_iso()
            self.save_state(state)

    def read_evidence(self, evidence_ref: str) -> str:
        if not evidence_ref:
            return ""
        target = self.evidence_dir / evidence_ref
        if not target.exists() or not target.is_file():
            return ""
        try:
            return target.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return ""

    def evidence_exists(self, evidence_ref: str) -> bool:
        if not evidence_ref:
            return True
        target = self.evidence_dir / evidence_ref
        return target.exists() and target.is_file()

    def get_evidence_content_by_key(self, key: str, revision: str | None = None) -> str:
        rec = self.get_record(key, revision)
        if rec is None:
            return ""
        return self.read_evidence(rec.evidence_ref)

    def append_event(
        self,
        *,
        event_type: str,
        bucket_id: str,
        key: str = "",
        revision_id: str = "",
        payload: dict | None = None,
    ) -> dict:
        if payload is None:
            payload = {}
        event = {
            "event_id": f"evt_{uuid4().hex}",
            "event_type": event_type,
            "bucket_id": bucket_id,
            "key": key,
            "revision_id": revision_id,
            "timestamp": utc_now_iso(),
            "payload": payload,
        }
        with self.events_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True))
            f.write("\n")
        return event

    def append_alias_audit(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        row = dict(payload)
        row.setdefault("timestamp", utc_now_iso())
        with self.alias_audit_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            f.write("\n")

    def load_events(self, limit: int | None = None) -> list[dict]:
        raw = self.events_file.read_text(encoding="utf-8").splitlines()
        events: list[dict] = []
        for line in raw:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                events.append(item)
        if limit is not None and limit > 0:
            return events[-limit:]
        return events

    def mark_bucket_dirty(self, bucket_id: str) -> dict:
        meta = self.load_meta()
        meta["dirty"] = True
        meta["context_version"] = int(meta.get("context_version", 0)) + 1
        versions = meta.get("bucket_versions", {})
        if not isinstance(versions, dict):
            versions = {}
        versions[bucket_id] = int(versions.get(bucket_id, 0)) + 1
        meta["bucket_versions"] = versions
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)
        return meta

    def clear_dirty(self) -> dict:
        meta = self.load_meta()
        meta["dirty"] = False
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)
        return meta

    def get_bucket_version(self, bucket_id: str) -> int:
        meta = self.load_meta()
        versions = meta.get("bucket_versions", {})
        if not isinstance(versions, dict):
            return 0
        return int(versions.get(bucket_id, 0))

    def record_llm_usage(self, *, input_tokens: int, output_tokens: int, cached_input_tokens: int, calls: int = 1) -> None:
        meta = self.load_meta()
        meta["llm_calls_total"] = int(meta.get("llm_calls_total", 0)) + max(0, int(calls))
        meta["llm_input_tokens_total"] = int(meta.get("llm_input_tokens_total", 0)) + max(0, int(input_tokens))
        meta["llm_output_tokens_total"] = int(meta.get("llm_output_tokens_total", 0)) + max(0, int(output_tokens))
        meta["llm_cached_input_tokens_total"] = int(meta.get("llm_cached_input_tokens_total", 0)) + max(
            0, int(cached_input_tokens)
        )
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_query_degraded(self) -> None:
        meta = self.load_meta()
        meta["degraded_query_total"] = int(meta.get("degraded_query_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_llm_parse_fail(self) -> None:
        meta = self.load_meta()
        meta["llm_parse_fail_total"] = int(meta.get("llm_parse_fail_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_llm_precheck_fail(self) -> None:
        meta = self.load_meta()
        meta["llm_precheck_fail_total"] = int(meta.get("llm_precheck_fail_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_clean_reject(self) -> None:
        meta = self.load_meta()
        meta["clean_reject_total"] = int(meta.get("clean_reject_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_clean_fallback(self) -> None:
        meta = self.load_meta()
        meta["clean_fallback_total"] = int(meta.get("clean_fallback_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_ingest_blocked_by_clean(self) -> None:
        meta = self.load_meta()
        meta["ingest_blocked_by_clean_total"] = int(meta.get("ingest_blocked_by_clean_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_context_overflow(self, stage: str) -> None:
        stage_key = str(stage or "").strip().lower()
        meta = self.load_meta()
        meta["context_overflow_total"] = int(meta.get("context_overflow_total", 0)) + 1
        if stage_key in {"query", "ingest", "compress"}:
            field = f"overflow_{stage_key}_total"
            meta[field] = int(meta.get(field, 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_file_import_reject(self) -> None:
        meta = self.load_meta()
        meta["file_import_reject_total"] = int(meta.get("file_import_reject_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_auto_split_guard_hit(self) -> None:
        meta = self.load_meta()
        meta["auto_split_guard_hit_total"] = int(meta.get("auto_split_guard_hit_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_auto_split_cooldown_skip(self) -> None:
        meta = self.load_meta()
        meta["auto_split_cooldown_skip_total"] = int(meta.get("auto_split_cooldown_skip_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_auto_split_no_progress(self) -> None:
        meta = self.load_meta()
        meta["auto_split_no_progress_total"] = int(meta.get("auto_split_no_progress_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_split_plan_warn(self) -> None:
        meta = self.load_meta()
        meta["split_plan_warn_total"] = int(meta.get("split_plan_warn_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_alias_real_key_leak(self) -> None:
        meta = self.load_meta()
        meta["real_key_leak_count"] = int(meta.get("real_key_leak_count", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_alias_resolve_fail(self) -> None:
        meta = self.load_meta()
        meta["alias_resolve_fail_count"] = int(meta.get("alias_resolve_fail_count", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_unknown_alias(self) -> None:
        meta = self.load_meta()
        meta["unknown_alias_count"] = int(meta.get("unknown_alias_count", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_query_alias_miss_build(self) -> None:
        meta = self.load_meta()
        meta["query_alias_miss_build_total"] = int(meta.get("query_alias_miss_build_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_query_alias_miss_resolve(self) -> None:
        meta = self.load_meta()
        meta["query_alias_miss_resolve_total"] = int(meta.get("query_alias_miss_resolve_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def record_query_side_effect_drop(self) -> None:
        meta = self.load_meta()
        meta["query_side_effect_drop_total"] = int(meta.get("query_side_effect_drop_total", 0)) + 1
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)

    def mark_auto_split(self, *, source_bucket_id: str, successor_bucket_id: str = "") -> None:
        meta = self.load_meta()
        now = utc_now_iso()
        by_bucket = meta.get("auto_split_last_at_by_bucket", {})
        if not isinstance(by_bucket, dict):
            by_bucket = {}
        by_bucket[source_bucket_id] = now
        meta["auto_split_last_at_by_bucket"] = by_bucket
        meta["last_auto_split_at"] = now
        meta["last_split_source_bucket_id"] = source_bucket_id
        meta["last_split_successor_bucket_id"] = successor_bucket_id
        meta["updated_at"] = now
        self.save_meta(meta)

    def get_last_auto_split_at(self, bucket_id: str) -> str:
        meta = self.load_meta()
        by_bucket = meta.get("auto_split_last_at_by_bucket", {})
        if not isinstance(by_bucket, dict):
            return ""
        return str(by_bucket.get(bucket_id, ""))

    def compute_cache_key(
        self,
        *,
        query_text: str,
        top_k: int,
        include_gray: bool,
        bucket_id: str,
        degraded_mode: bool,
        mode: str = "auto",
        global_recall_top_n: int = 0,
        global_recall_top_m: int = 0,
        global_recall_depth_limit: int = 0,
        global_recall_time_budget_ms: int = 0,
    ) -> str:
        meta = self.load_meta()
        payload = {
            "query_text": query_text,
            "top_k": top_k,
            "include_gray": include_gray,
            "bucket_id": bucket_id,
            "bucket_version": self.get_bucket_version(bucket_id),
            "degraded_mode": degraded_mode,
            "mode": str(mode or "auto"),
            "global_recall_top_n": int(global_recall_top_n),
            "global_recall_top_m": int(global_recall_top_m),
            "global_recall_depth_limit": int(global_recall_depth_limit),
            "global_recall_time_budget_ms": int(global_recall_time_budget_ms),
            "context_version": int(meta.get("context_version", 0)),
            "prompt_version": str(meta.get("prompt_version", self._prompt_version)),
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def get_query_cache(self, cache_key: str) -> dict | None:
        cache = self.load_cache()
        hit = cache.get(cache_key)
        return hit if isinstance(hit, dict) else None

    def set_query_cache(self, cache_key: str, result: dict, *, bucket_id: str) -> None:
        cache = self.load_cache()
        cache[cache_key] = {
            "created_at": utc_now_iso(),
            "bucket_id": bucket_id,
            "bucket_version": self.get_bucket_version(bucket_id),
            "result": result,
        }
        if len(cache) > 5000:
            keys = list(cache.keys())[: len(cache) - 5000]
            for k in keys:
                cache.pop(k, None)
        self.save_cache(cache)

    def estimate_bucket_tokens(self, bucket_id: str, *, include_gray: bool = True) -> int:
        total_chars = 0
        for rec in self.list_bucket_records(bucket_id, include_gray=include_gray):
            total_chars += len(rec.title) + len(rec.summary) + len(rec.content)
            for rel_name, rel_items in rec.relations.items():
                total_chars += len(rel_name)
                for item in rel_items:
                    total_chars += len(json.dumps(item, ensure_ascii=False))
        return max(1, total_chars // 3)

    def create_snapshot(self, *, summary: str, bucket_id: str, reason: str, keep_keys: list[str], drop_keys: list[str]) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = self.snapshots_dir / f"snapshot_{bucket_id}_{stamp}.md"
        lines = [
            "# Context Memory V3 Snapshot",
            "",
            f"- created_at: {utc_now_iso()}",
            f"- bucket_id: {bucket_id}",
            f"- reason: {reason}",
            "",
            "## Summary",
            "",
            summary.strip() if summary.strip() else "(empty)",
            "",
            "## Keep Keys",
            "",
        ]
        lines.extend([f"- {k}" for k in keep_keys] if keep_keys else ["- (none)"])
        lines.extend(["", "## Drop Keys", ""])
        lines.extend([f"- {k}" for k in drop_keys] if drop_keys else ["- (none)"])
        path.write_text("\n".join(lines), encoding="utf-8")

        meta = self.load_meta()
        meta["last_snapshot"] = self._to_relative_root_path(path)
        meta["updated_at"] = utc_now_iso()
        self.save_meta(meta)
        return str(path)

    def _job_file_path(self, batch_id: str) -> Path:
        safe_id = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(batch_id).strip())
        return self.jobs_dir / f"job_{safe_id}.json"

    def save_job_journal(self, job: dict[str, Any]) -> None:
        if not isinstance(job, dict):
            return
        batch_id = str(job.get("batch_id", "")).strip()
        if not batch_id:
            return
        payload = dict(job)
        payload["updated_at"] = utc_now_iso()
        self._atomic_save_json(payload, self._job_file_path(batch_id))

    def load_job_journal(self, batch_id: str) -> dict[str, Any] | None:
        path = self._job_file_path(batch_id)
        if not path.exists():
            return None
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(loaded, dict):
            return None
        if not str(loaded.get("batch_id", "")).strip():
            loaded["batch_id"] = str(batch_id)
        return loaded

    def list_job_journals(self, *, statuses: set[str] | None = None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for path in self.jobs_dir.glob("job_*.json"):
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(loaded, dict):
                continue
            if statuses is not None:
                status = str(loaded.get("status", "")).strip().lower()
                if status not in statuses:
                    continue
            out.append(loaded)
        out.sort(key=lambda x: str(x.get("updated_at", "")))
        return out

    def migrate_paths_to_relative(self) -> dict[str, int]:
        changed_state = 0
        changed_meta = 0

        state = self.load_state()
        keys = state.get("keys", {})
        if isinstance(keys, dict):
            for node in keys.values():
                if not isinstance(node, dict):
                    continue
                path_text = node.get("latest_path")
                if not isinstance(path_text, str) or not path_text.strip():
                    continue
                resolved = Path(path_text)
                if not resolved.is_absolute():
                    continue
                try:
                    rel = resolved.resolve().relative_to(self.root_dir.resolve())
                except Exception:
                    continue
                node["latest_path"] = str(rel)
                changed_state += 1
            if changed_state > 0:
                self.save_state(state)

        meta = self.load_meta()
        snap = meta.get("last_snapshot")
        if isinstance(snap, str) and snap.strip():
            snap_path = Path(snap)
            if snap_path.is_absolute():
                try:
                    rel_snap = snap_path.resolve().relative_to(self.root_dir.resolve())
                    meta["last_snapshot"] = str(rel_snap)
                    changed_meta = 1
                    self.save_meta(meta)
                except Exception:
                    pass

        return {"state_latest_path_changed": changed_state, "meta_last_snapshot_changed": changed_meta}

    def get_stats(self) -> dict[str, Any]:
        state = self.load_state()
        meta = self.load_meta()
        cache = self.load_cache()
        keys = state.get("keys", {})
        total_keys = len(keys)
        gray_keys = 0
        active_keys = 0
        for _, node in keys.items():
            if not isinstance(node, dict):
                continue
            if bool(node.get("gray", False)):
                gray_keys += 1
            else:
                active_keys += 1

        tree = self.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            buckets = {}

        return {
            "total_keys": total_keys,
            "active_keys": active_keys,
            "gray_keys": gray_keys,
            "revision_total": int(state.get("revision_total", 0)),
            "event_total": len(self.load_events()),
            "cache_entries": len(cache),
            "dirty": bool(meta.get("dirty", False)),
            "context_version": int(meta.get("context_version", 0)),
            "latest_snapshot": str(meta.get("last_snapshot", "")),
            "llm_calls_total": int(meta.get("llm_calls_total", 0)),
            "llm_input_tokens_total": int(meta.get("llm_input_tokens_total", 0)),
            "llm_output_tokens_total": int(meta.get("llm_output_tokens_total", 0)),
            "llm_cached_input_tokens_total": int(meta.get("llm_cached_input_tokens_total", 0)),
            "degraded_query_total": int(meta.get("degraded_query_total", 0)),
            "llm_parse_fail_total": int(meta.get("llm_parse_fail_total", 0)),
            "llm_precheck_fail_total": int(meta.get("llm_precheck_fail_total", 0)),
            "clean_reject_total": int(meta.get("clean_reject_total", 0)),
            "clean_fallback_total": int(meta.get("clean_fallback_total", 0)),
            "ingest_blocked_by_clean_total": int(meta.get("ingest_blocked_by_clean_total", 0)),
            "context_overflow_total": int(meta.get("context_overflow_total", 0)),
            "overflow_query_total": int(meta.get("overflow_query_total", 0)),
            "overflow_ingest_total": int(meta.get("overflow_ingest_total", 0)),
            "overflow_compress_total": int(meta.get("overflow_compress_total", 0)),
            "file_import_reject_total": int(meta.get("file_import_reject_total", 0)),
            "auto_split_guard_hit_total": int(meta.get("auto_split_guard_hit_total", 0)),
            "auto_split_cooldown_skip_total": int(meta.get("auto_split_cooldown_skip_total", 0)),
            "auto_split_no_progress_total": int(meta.get("auto_split_no_progress_total", 0)),
            "split_plan_warn_total": int(meta.get("split_plan_warn_total", 0)),
            "last_auto_split_at": str(meta.get("last_auto_split_at", "")),
            "last_split_source_bucket_id": str(meta.get("last_split_source_bucket_id", "")),
            "last_split_successor_bucket_id": str(meta.get("last_split_successor_bucket_id", "")),
            "real_key_leak_count": int(meta.get("real_key_leak_count", 0)),
            "alias_resolve_fail_count": int(meta.get("alias_resolve_fail_count", 0)),
            "unknown_alias_count": int(meta.get("unknown_alias_count", 0)),
            "query_alias_miss_build_total": int(meta.get("query_alias_miss_build_total", 0)),
            "query_alias_miss_resolve_total": int(meta.get("query_alias_miss_resolve_total", 0)),
            "query_side_effect_drop_total": int(meta.get("query_side_effect_drop_total", 0)),
            "root_bucket_id": str(tree.get("root_bucket_id", "")),
            "active_bucket_id": str(tree.get("active_bucket_id", "")),
            "bucket_total": len(buckets),
        }

    def _json_to_memory_record(self, path: Path) -> MemoryRecord | None:
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        data["relations"] = normalize_relations(data.get("relations", {}))
        return MemoryRecord.from_dict(data)

    def apply_negative_penalty(self, key: str, value: float) -> None:
        # Persisted for observability if needed in future.
        state = self.load_state()
        node = state.get("keys", {}).get(key)
        if not isinstance(node, dict):
            return
        node["last_negative_weight"] = float(value)
        node["updated_at"] = utc_now_iso()
        self.save_state(state)

    def move_record_to_bucket(self, key: str, bucket_id: str) -> None:
        state = self.load_state()
        node = state.get("keys", {}).get(key)
        if not isinstance(node, dict):
            return
        node["bucket_id"] = bucket_id
        node["updated_at"] = utc_now_iso()
        self.save_state(state)

