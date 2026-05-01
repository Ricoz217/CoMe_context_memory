from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from typing import Any


KEY_TYPE_MEMORY = "memory"
KEY_TYPE_BUCKET = "bucket"
KEY_TYPE_REVISION = "revision"
KEY_TYPE_REF = "ref"

KEY_TYPES = {
    KEY_TYPE_MEMORY,
    KEY_TYPE_BUCKET,
    KEY_TYPE_REVISION,
    KEY_TYPE_REF,
}

_ALIAS_RE = re.compile(r"^(memory|bucket|revision|ref)_[1-9]\d*$")
_REAL_MEMORY_RE = re.compile(r"^mem_[0-9]{14}_[0-9a-f]{32}$")
_REAL_BUCKET_RE = re.compile(r"^bucket_[0-9]{14}_[0-9a-f]{32}$")
_REAL_REVISION_RE = re.compile(r"^rev_[0-9]{14}_[0-9a-f]{32}$")

_FIELD_TYPES: dict[str, str] = {
    "key": KEY_TYPE_MEMORY,
    # node-like fields can point to either memory nodes or bucket nodes depending on payload semantics.
    "node_key": KEY_TYPE_REF,
    "parent_node_key": KEY_TYPE_REF,
    "bucket_id": KEY_TYPE_BUCKET,
    "child_bucket_id": KEY_TYPE_BUCKET,
    "source_bucket_id": KEY_TYPE_BUCKET,
    "successor_bucket_id": KEY_TYPE_BUCKET,
    "target_bucket_id": KEY_TYPE_BUCKET,
    "current_bucket_id": KEY_TYPE_BUCKET,
    "from_bucket": KEY_TYPE_BUCKET,
    "old_child_bucket_id": KEY_TYPE_BUCKET,
    "new_child_bucket_id": KEY_TYPE_BUCKET,
    "revision_id": KEY_TYPE_REVISION,
    "from_revision": KEY_TYPE_REVISION,
    "split_key_prev": KEY_TYPE_MEMORY,
    "split_key_next": KEY_TYPE_MEMORY,
    "group_key": KEY_TYPE_BUCKET,
    "group_bucket_id": KEY_TYPE_BUCKET,
    "parent_key": KEY_TYPE_REF,
}

_LIST_FIELD_TYPES: dict[str, str] = {
    "key_hints": KEY_TYPE_REF,
    "split_keys": KEY_TYPE_MEMORY,
    "keep_keys": KEY_TYPE_MEMORY,
    "drop_keys": KEY_TYPE_MEMORY,
    "keys": KEY_TYPE_MEMORY,
    "parent_keys": KEY_TYPE_MEMORY,
    "parent_flat_keys": KEY_TYPE_REF,
    "members": KEY_TYPE_REF,
    "memory_keys": KEY_TYPE_MEMORY,
    "leaf_nodes": KEY_TYPE_REF,
    "bucket_refs": KEY_TYPE_BUCKET,
    "child_keys": KEY_TYPE_MEMORY,
}


class AliasPayloadError(RuntimeError):
    pass


def _normalize_key_type(key_type: str) -> str:
    t = str(key_type or "").strip().lower()
    if t not in KEY_TYPES:
        raise ValueError(f"unsupported key_type: {key_type}")
    return t


def looks_like_alias(value: str) -> bool:
    return bool(_ALIAS_RE.match(str(value or "").strip()))


def infer_real_key_type(value: str) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    if _REAL_MEMORY_RE.match(s):
        return KEY_TYPE_MEMORY
    if _REAL_BUCKET_RE.match(s):
        return KEY_TYPE_BUCKET
    if _REAL_REVISION_RE.match(s):
        return KEY_TYPE_REVISION
    if s.startswith("ref_"):
        return KEY_TYPE_REF
    return ""


def looks_like_real_key(value: str) -> bool:
    if looks_like_alias(value):
        return False
    return bool(infer_real_key_type(value))


def stable_payload_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(raw.encode("utf-8")).hexdigest()


@dataclass
class AliasCodec:
    storage: Any

    def get_or_create_alias(self, bucket_id: str, real_key: str, key_type: str) -> str:
        return self.storage.get_or_create_alias(bucket_id, real_key, _normalize_key_type(key_type))

    def resolve_alias(self, bucket_id: str, alias: str, expected_type: str | None = None) -> str:
        et = _normalize_key_type(expected_type) if expected_type else None
        return self.storage.resolve_alias(bucket_id, alias, et)

    def freeze_alias_map(self, bucket_id: str) -> None:
        self.storage.freeze_alias_map(bucket_id)

    def alias_map_version(self, bucket_id: str) -> int:
        return int(self.storage.alias_map_version(bucket_id))

    def build_llm_view(
        self,
        bucket_id: str,
        real_payload: Any,
        map_version: int | None = None,
        *,
        allow_create: bool = True,
    ) -> Any:
        if map_version is not None and int(map_version) != self.alias_map_version(bucket_id):
            raise AliasPayloadError(f"alias map version changed: expect={map_version}, got={self.alias_map_version(bucket_id)}")
        return self._walk_to_alias(bucket_id, real_payload, "", allow_create=allow_create)

    def resolve_llm_output(self, bucket_id: str, alias_output: Any, map_version: int | None = None) -> Any:
        if map_version is not None and int(map_version) != self.alias_map_version(bucket_id):
            raise AliasPayloadError(f"alias map version changed: expect={map_version}, got={self.alias_map_version(bucket_id)}")
        return self._walk_to_real(bucket_id, alias_output, "")

    def assert_alias_only_payload(self, bucket_id: str, payload: Any) -> None:
        leak = self._find_real_key_leak(payload, "")
        if leak:
            self.storage.record_alias_real_key_leak()
            raise AliasPayloadError(f"real key leaked in alias payload for bucket={bucket_id}: {leak}")

    def _walk_to_alias(self, bucket_id: str, value: Any, parent_key: str, *, allow_create: bool) -> Any:
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for k, v in value.items():
                sk = str(k)
                key_out = sk
                if parent_key == "children":
                    key_out = str(self._to_alias_scalar(bucket_id, sk, KEY_TYPE_REF, allow_create=allow_create))
                lk = sk.lower()
                if lk == "relations" and isinstance(v, dict):
                    out[key_out] = self._aliasize_relations(bucket_id, v, allow_create=allow_create)
                    continue
                if lk in _FIELD_TYPES:
                    out[key_out] = self._to_alias_scalar(bucket_id, v, _FIELD_TYPES[lk], allow_create=allow_create)
                    continue
                if lk in _LIST_FIELD_TYPES and isinstance(v, list):
                    et = _LIST_FIELD_TYPES[lk]
                    out[key_out] = [self._to_alias_scalar(bucket_id, x, et, allow_create=allow_create) for x in v]
                    continue
                out[key_out] = self._walk_to_alias(bucket_id, v, lk, allow_create=allow_create)
            return out
        if isinstance(value, list):
            return [self._walk_to_alias(bucket_id, x, parent_key, allow_create=allow_create) for x in value]
        return value

    def _walk_to_real(self, bucket_id: str, value: Any, parent_key: str) -> Any:
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for k, v in value.items():
                sk = str(k)
                key_out = sk
                if parent_key == "children":
                    key_out = str(self._to_real_scalar(bucket_id, sk, KEY_TYPE_REF, strict=True))
                lk = sk.lower()
                if lk == "relations" and isinstance(v, dict):
                    out[key_out] = self._realize_relations(bucket_id, v)
                    continue
                if lk in _FIELD_TYPES:
                    out[key_out] = self._to_real_scalar(bucket_id, v, _FIELD_TYPES[lk], strict=True)
                    continue
                if lk in _LIST_FIELD_TYPES and isinstance(v, list):
                    et = _LIST_FIELD_TYPES[lk]
                    out[key_out] = [self._to_real_scalar(bucket_id, x, et, strict=True) for x in v]
                    continue
                out[key_out] = self._walk_to_real(bucket_id, v, lk)
            return out
        if isinstance(value, list):
            return [self._walk_to_real(bucket_id, x, parent_key) for x in value]
        return value

    def _aliasize_relations(self, bucket_id: str, relations: dict[str, Any], *, allow_create: bool) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for cat, items in relations.items():
            if not isinstance(items, list):
                out[str(cat)] = []
                continue
            converted: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                row = dict(item)
                target = str(row.get("target", "")).strip()
                if target:
                    inferred = infer_real_key_type(target)
                    if inferred:
                        row["target"] = self._to_alias_scalar(bucket_id, target, inferred, allow_create=allow_create)
                converted.append(row)
            out[str(cat)] = converted
        return out

    def _realize_relations(self, bucket_id: str, relations: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for cat, items in relations.items():
            if not isinstance(items, list):
                out[str(cat)] = []
                continue
            converted: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                row = dict(item)
                target = str(row.get("target", "")).strip()
                if target:
                    if looks_like_alias(target):
                        try:
                            row["target"] = self.resolve_alias(bucket_id, target, None)
                        except Exception:
                            self.storage.record_alias_resolve_fail()
                            self.storage.record_unknown_alias()
                            raise AliasPayloadError(f"unknown alias target: {target}")
                    elif looks_like_real_key(target):
                        self.storage.record_alias_real_key_leak()
                        raise AliasPayloadError(f"real key returned in relation target: {target}")
                converted.append(row)
            out[str(cat)] = converted
        return out

    def _to_alias_scalar(self, bucket_id: str, value: Any, expected_type: str, *, allow_create: bool) -> Any:
        if not isinstance(value, str):
            return value
        s = value.strip()
        if not s:
            return value
        if looks_like_alias(s):
            return s
        inferred = infer_real_key_type(s)
        if inferred:
            if allow_create:
                return self.get_or_create_alias(bucket_id, s, inferred)
            found = self.storage.find_alias(bucket_id, s, inferred)
            if found:
                return found
            raise AliasPayloadError(f"missing alias for {inferred}:{s}")
        if expected_type == KEY_TYPE_REF:
            if allow_create:
                return self.get_or_create_alias(bucket_id, s, KEY_TYPE_REF)
            found = self.storage.find_alias(bucket_id, s, KEY_TYPE_REF)
            if found:
                return found
            raise AliasPayloadError(f"missing alias for ref:{s}")
        return value

    def _to_real_scalar(self, bucket_id: str, value: Any, expected_type: str, strict: bool) -> Any:
        if not isinstance(value, str):
            return value
        s = value.strip()
        if not s:
            return value
        if looks_like_alias(s):
            try:
                if expected_type == KEY_TYPE_REF:
                    return self.resolve_alias(bucket_id, s, None)
                return self.resolve_alias(bucket_id, s, expected_type)
            except Exception:
                self.storage.record_alias_resolve_fail()
                self.storage.record_unknown_alias()
                raise AliasPayloadError(f"failed to resolve alias {s} as {expected_type}")
        if strict:
            if looks_like_real_key(s):
                self.storage.record_alias_real_key_leak()
                raise AliasPayloadError(f"real key returned from llm in alias-only mode: {s}")
            self.storage.record_alias_resolve_fail()
            raise AliasPayloadError(f"non-alias value in alias-only field: {s}")
        return value

    def _find_real_key_leak(self, payload: Any, parent_key: str) -> str:
        if isinstance(payload, dict):
            for k, v in payload.items():
                lk = str(k).lower()
                if parent_key == "children" and looks_like_real_key(str(k).strip()):
                    return f"children.key={k}"
                if lk in _FIELD_TYPES and isinstance(v, str) and looks_like_real_key(v):
                    return f"{lk}={v}"
                if lk == "metadata_update" and isinstance(v, dict):
                    for mk in v.keys():
                        mks = str(mk).strip()
                        if looks_like_real_key(mks):
                            return f"metadata_update.key={mks}"
                if lk in _LIST_FIELD_TYPES and isinstance(v, list):
                    for item in v:
                        if isinstance(item, str) and looks_like_real_key(item):
                            return f"{lk}[]={item}"
                if lk == "relations" and isinstance(v, dict):
                    for cat, rows in v.items():
                        if not isinstance(rows, list):
                            continue
                        for row in rows:
                            if not isinstance(row, dict):
                                continue
                            target = row.get("target")
                            if isinstance(target, str) and looks_like_real_key(target):
                                return f"relations.{cat}.target={target}"
                leak = self._find_real_key_leak(v, lk)
                if leak:
                    return leak
            return ""
        if isinstance(payload, list):
            for item in payload:
                leak = self._find_real_key_leak(item, parent_key)
                if leak:
                    return leak
        return ""
