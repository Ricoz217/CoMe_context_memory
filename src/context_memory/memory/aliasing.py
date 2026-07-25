from __future__ import annotations

import asyncio
import copy
import json
import re
import threading
from collections import ChainMap
from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
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
_ALIAS_TOKEN_RE = re.compile(r"^(memory|bucket|revision|ref)_([1-9]\d*)$")
_REAL_MEMORY_RE = re.compile(r"^mem_[0-9]{14}_[0-9a-f]{32}$")
_REAL_BUCKET_RE = re.compile(r"^bucket_[0-9]{14}_[0-9a-f]{32}$")
_REAL_REVISION_RE = re.compile(r"^rev_[0-9]{14}_[0-9a-f]{32}$")
_REAL_ID_IN_TEXT_RE = re.compile(r"(?:mem|bucket|rev)_[0-9]{14}_[0-9a-f]{32}")

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
    # "keys" appears in split/optimize planning payloads where model may
    # reference either memory-node keys or bucket ids. Use ref to allow both.
    "keys": KEY_TYPE_REF,
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


def validate_alias_map_payload(
    payload: dict[str, Any],
    *,
    bucket_id: str | None = None,
    normalize_metadata: bool = False,
    _copy_payload: bool = True,
) -> tuple[dict[str, Any], bool]:
    """Validate an alias map and optionally repair metadata without changing mappings."""
    if not isinstance(payload, dict):
        raise AliasPayloadError("invalid alias map structure")

    normalized = copy.deepcopy(payload) if _copy_payload else payload
    changed = False
    real_to_alias = normalized.get("real_to_alias")
    alias_to_real = normalized.get("alias_to_real")
    counters = normalized.get("counters")
    if not isinstance(real_to_alias, dict) or not isinstance(alias_to_real, dict):
        raise AliasPayloadError("invalid alias map structure")
    if not isinstance(counters, dict):
        if not normalize_metadata:
            raise AliasPayloadError("invalid alias map structure")
        counters = {}
        normalized["counters"] = counters
        changed = True

    maxima = {"memory": 0, "bucket": 0, "revision": 0, "ref": 0}
    for typed_key, alias_raw in real_to_alias.items():
        key_type, separator, real_id = str(typed_key).partition(":")
        alias = str(alias_raw)
        reverse = alias_to_real.get(alias)
        match = _ALIAS_TOKEN_RE.fullmatch(alias)
        if key_type not in maxima or not separator or not real_id or match is None or match.group(1) != key_type:
            raise AliasPayloadError(f"invalid alias mapping: {typed_key}")
        if key_type != KEY_TYPE_REF:
            try:
                inferred_type = infer_real_key_type(real_id)
            except ValueError as exc:
                raise AliasPayloadError(f"invalid real id for alias mapping: {typed_key}") from exc
            if inferred_type != key_type:
                raise AliasPayloadError(f"invalid real id for alias mapping: {typed_key}")
        if not isinstance(reverse, dict):
            raise AliasPayloadError(f"invalid alias mapping: {typed_key}")
        if str(reverse.get("key_type", "")) != key_type or str(reverse.get("real_key", "")) != real_id:
            raise AliasPayloadError(f"inconsistent alias mapping: {typed_key}")
        maxima[key_type] = max(maxima[key_type], int(match.group(2)))

    for alias, reverse in alias_to_real.items():
        if not isinstance(reverse, dict):
            raise AliasPayloadError(f"invalid reverse alias mapping: {alias}")
        key_type = str(reverse.get("key_type", ""))
        real_id = str(reverse.get("real_key", ""))
        match = _ALIAS_TOKEN_RE.fullmatch(str(alias))
        if key_type not in maxima or not real_id or match is None or match.group(1) != key_type:
            raise AliasPayloadError(f"invalid reverse alias mapping: {alias}")
        if key_type != KEY_TYPE_REF:
            try:
                inferred_type = infer_real_key_type(real_id)
            except ValueError as exc:
                raise AliasPayloadError(f"invalid reverse alias real id: {alias}") from exc
            if inferred_type != key_type:
                raise AliasPayloadError(f"invalid reverse alias real id: {alias}")
        typed_key = f"{key_type}:{real_id}"
        if str(real_to_alias.get(typed_key, "")) != str(alias):
            raise AliasPayloadError(f"orphan reverse alias mapping: {alias}")
        maxima[key_type] = max(maxima[key_type], int(match.group(2)))

    for key_type, maximum in maxima.items():
        try:
            current = max(0, int(counters.get(key_type, 0)))
        except Exception as exc:
            raise AliasPayloadError(f"invalid alias counter: {key_type}") from exc
        if current < maximum:
            if not normalize_metadata:
                raise AliasPayloadError(f"alias counter behind mappings: {key_type}")
            current = maximum
        if counters.get(key_type) != current:
            if not normalize_metadata:
                raise AliasPayloadError(f"invalid alias counter: {key_type}")
            counters[key_type] = current
            changed = True

    expected_bucket = str(bucket_id or "").strip()
    current_bucket = str(normalized.get("bucket_id", "")).strip()
    if expected_bucket and current_bucket != expected_bucket:
        if not normalize_metadata or current_bucket:
            raise AliasPayloadError(f"alias map bucket mismatch: expect={expected_bucket}, got={current_bucket}")
        normalized["bucket_id"] = expected_bucket
        changed = True

    if "map_version" not in normalized:
        if not normalize_metadata:
            raise AliasPayloadError("invalid alias map version")
        normalized["map_version"] = 1
        changed = True
    else:
        try:
            map_version = int(normalized["map_version"])
        except Exception as exc:
            raise AliasPayloadError("invalid alias map version") from exc
        if map_version < 1:
            raise AliasPayloadError("invalid alias map version")
        if normalized["map_version"] != map_version:
            if not normalize_metadata:
                raise AliasPayloadError("invalid alias map version")
            normalized["map_version"] = map_version
            changed = True

    if "sealed" not in normalized:
        if not normalize_metadata:
            raise AliasPayloadError("invalid alias sealed flag")
        normalized["sealed"] = False
        changed = True
    elif not isinstance(normalized["sealed"], bool):
        raise AliasPayloadError("invalid alias sealed flag")

    return normalized, changed


class AliasTable:
    """Thread-safe alias mapping for one resolved bucket."""

    def __init__(self, storage: Any, bucket_id: str) -> None:
        self.storage = storage
        self.bucket_id = str(bucket_id or "").strip()
        if not self.bucket_id:
            raise ValueError("bucket_id is empty")
        lock_factory = getattr(storage, "get_alias_map_lock", None)
        self._lock = lock_factory(self.bucket_id) if callable(lock_factory) else threading.RLock()

    def to_alias(self, real_id: str, *, key_type: str | None = None, allow_create: bool = True) -> str:
        token = str(real_id or "").strip()
        inferred = infer_real_key_type(token)
        resolved_type = _normalize_key_type(key_type) if key_type else inferred
        if not resolved_type:
            raise ValueError(f"invalid real id: {real_id}")
        if inferred and key_type and resolved_type != KEY_TYPE_REF and inferred != resolved_type:
            raise ValueError(f"real id type mismatch: expected={resolved_type}, got={inferred}")
        with self._lock:
            if self._supports_transactions():
                encoded = self._encode_tree_transaction(token, allow_create=allow_create, forced_type=resolved_type)
                return str(encoded)
            if allow_create:
                return str(self.storage.get_or_create_alias(self.bucket_id, token, resolved_type))
            found = self.storage.find_alias(self.bucket_id, token, resolved_type)
            if found:
                return str(found)
            raise AliasPayloadError(f"missing alias for {resolved_type}:{token}")

    def to_real(self, alias: str, *, expected_type: str | None = None) -> str:
        token = str(alias or "").strip()
        if not token:
            raise ValueError("alias is empty")
        with self._lock:
            return str(self.storage.resolve_alias(self.bucket_id, token, expected_type))

    def to_real_many(
        self,
        aliases: Iterable[str],
        *,
        expected_type: str | None = None,
        strict: bool = False,
    ) -> dict[str, str]:
        """Resolve aliases under one table lock, optionally skipping invalid entries."""
        if isinstance(aliases, (str, bytes)):
            raise TypeError("aliases must be an iterable of alias strings, not a string")
        resolved: dict[str, str] = {}
        seen: set[str] = set()
        with self._lock:
            amap: dict[str, Any] | None = None
            if self._supports_transactions():
                amap = self.storage.load_alias_map(self.bucket_id)
                self._validate_map(amap)
            for alias in aliases:
                token = str(alias or "").strip()
                if not token:
                    if strict:
                        raise ValueError("alias is empty")
                    continue
                if token in seen:
                    continue
                seen.add(token)
                try:
                    if amap is not None:
                        real_id = self._real_from_map(token, amap, expected_type=expected_type)
                    else:
                        real_id = self.storage.resolve_alias(self.bucket_id, token, expected_type)
                except (KeyError, TypeError):
                    if strict:
                        raise
                    continue
                resolved[token] = str(real_id)
        return resolved

    async def resolve_many(
        self,
        aliases: Iterable[str],
        *,
        expected_type: str | None = None,
        strict: bool = False,
    ) -> dict[str, str]:
        if isinstance(aliases, (str, bytes)):
            raise TypeError("aliases must be an iterable of alias strings, not a string")
        alias_batch = tuple(aliases)
        return await asyncio.to_thread(
            self.to_real_many,
            alias_batch,
            expected_type=expected_type,
            strict=strict,
        )

    def encode_text(self, text: str, *, allow_create: bool = True) -> str:
        with self._lock:
            return str(self._encode_tree_transaction(str(text), allow_create=allow_create))

    def encode_tree(
        self,
        value: Any,
        *,
        allow_create: bool = True,
        map_version: int | None = None,
    ) -> Any:
        with self._lock:
            encoded = self._encode_tree_transaction(
                value,
                allow_create=allow_create,
                map_version=map_version,
            )
        self.assert_safe(encoded)
        return encoded

    def encode_tree_with_version(
        self,
        value: Any,
        *,
        allow_create: bool = True,
        map_version: int | None = None,
    ) -> tuple[Any, int]:
        """Encode one value and return the exact committed map version."""
        with self._lock:
            encoded, committed_version = self._encode_tree_transaction_with_version(
                value,
                allow_create=allow_create,
                map_version=map_version,
            )
        self.assert_safe(encoded)
        return encoded, committed_version

    def encode_many(
        self,
        values: Iterable[Any],
        *,
        allow_create: bool = True,
        strict: bool = True,
        map_version: int | None = None,
    ) -> tuple[tuple[bool, Any], ...]:
        """Encode many independent values against one alias-map snapshot."""
        if isinstance(values, (str, bytes)):
            raise TypeError("values must be an iterable, not a string")
        if allow_create and not strict:
            raise ValueError("best-effort alias encoding cannot allocate new aliases")
        batch = tuple(values)
        with self._lock:
            if not self._supports_transactions():
                results: list[tuple[bool, Any]] = []
                for value in batch:
                    try:
                        encoded = self._encode_with_callbacks(value, allow_create=allow_create)
                    except (AliasPayloadError, ValueError):
                        if strict:
                            raise
                        results.append((False, None))
                        continue
                    results.append((True, encoded))
            else:
                original = self.storage.load_alias_map(self.bucket_id)
                self._validate_map(original)
                self._assert_map_version(original, map_version)
                transaction = self._new_delta_transaction(original)
                added = [0]
                results = []
                for value in batch:
                    try:
                        encoded = self._encode_value(
                            value,
                            transaction,
                            allow_create=allow_create,
                            added=added,
                        )
                    except (AliasPayloadError, ValueError):
                        if strict:
                            raise
                        results.append((False, None))
                        continue
                    results.append((True, encoded))
                if added[0] > 0:
                    snapshot = self._materialize_delta(original, transaction)
                    commit = getattr(self.storage, "commit_alias_map", None)
                    if callable(commit):
                        commit(self.bucket_id, snapshot)
                    else:
                        self.storage.save_alias_map(self.bucket_id, snapshot)
        for success, encoded in results:
            if success:
                self.assert_safe(encoded)
        return tuple(results)

    def decode_tree(
        self,
        value: Any,
        *,
        map_version: int | None = None,
        strict_unknown: bool = True,
    ) -> Any:
        """Restore exact alias tokens in an arbitrary structured value."""
        with self._lock:
            amap = self.storage.load_alias_map(self.bucket_id)
            self._validate_map(amap)
            self._assert_map_version(amap, map_version)
            return self._decode_value(value, amap, strict_unknown=strict_unknown)

    async def prepare(
        self,
        value: Any,
        *,
        allow_create: bool = True,
        map_version: int | None = None,
    ) -> Any:
        return await asyncio.to_thread(
            self.encode_tree,
            value,
            allow_create=allow_create,
            map_version=map_version,
        )

    async def restore(
        self,
        value: Any,
        *,
        map_version: int | None = None,
        strict_unknown: bool = True,
    ) -> Any:
        return await asyncio.to_thread(
            self.decode_tree,
            value,
            map_version=map_version,
            strict_unknown=strict_unknown,
        )

    def assert_safe(self, value: Any) -> None:
        leak = self._find_leak(value, "$")
        if not leak:
            return
        record = getattr(self.storage, "record_alias_real_key_leak", None)
        if callable(record):
            record()
        raise AliasPayloadError(f"real id leaked in alias payload for bucket={self.bucket_id}: {leak}")

    def map_version(self) -> int:
        return int(self.storage.alias_map_version(self.bucket_id))

    def freeze(self) -> None:
        with self._lock:
            self.storage.freeze_alias_map(self.bucket_id)

    def snapshot_hash(self) -> str:
        with self._lock:
            amap = self.storage.load_alias_map(self.bucket_id)
            self._validate_map(amap)
            return stable_payload_hash(amap)

    def _supports_transactions(self) -> bool:
        return callable(getattr(self.storage, "load_alias_map", None)) and (
            callable(getattr(self.storage, "commit_alias_map", None))
            or callable(getattr(self.storage, "save_alias_map", None))
        )

    def _encode_tree_transaction(
        self,
        value: Any,
        *,
        allow_create: bool = True,
        forced_type: str | None = None,
        map_version: int | None = None,
    ) -> Any:
        encoded, _ = self._encode_tree_transaction_with_version(
            value,
            allow_create=allow_create,
            forced_type=forced_type,
            map_version=map_version,
        )
        return encoded

    def _encode_tree_transaction_with_version(
        self,
        value: Any,
        *,
        allow_create: bool = True,
        forced_type: str | None = None,
        map_version: int | None = None,
    ) -> tuple[Any, int]:
        if not self._supports_transactions():
            encoded = self._encode_with_callbacks(value, allow_create=allow_create, forced_type=forced_type)
            return encoded, int(self.storage.alias_map_version(self.bucket_id))

        original = self.storage.load_alias_map(self.bucket_id)
        self._validate_map(original)
        self._assert_map_version(original, map_version)
        transaction = self._new_delta_transaction(original)
        added = [0]
        encoded = self._encode_value(
            value,
            transaction,
            allow_create=allow_create,
            added=added,
            forced_type=forced_type,
        )
        if added[0] > 0:
            snapshot = self._materialize_delta(original, transaction)
            commit = getattr(self.storage, "commit_alias_map", None)
            if callable(commit):
                commit(self.bucket_id, snapshot)
            else:
                self.storage.save_alias_map(self.bucket_id, snapshot)
            committed_version = int(snapshot.get("map_version", 1))
        else:
            committed_version = int(original.get("map_version", 1))
        return encoded, committed_version

    @staticmethod
    def _new_delta_transaction(original: dict[str, Any]) -> dict[str, Any]:
        """Create a copy-on-write view backed by small pending dictionaries."""
        return {
            "bucket_id": original.get("bucket_id", ""),
            "map_version": int(original.get("map_version", 1)),
            "sealed": bool(original.get("sealed", False)),
            "real_to_alias": ChainMap(
                {},
                MappingProxyType(original["real_to_alias"]),
            ),
            "alias_to_real": ChainMap(
                {},
                MappingProxyType(original["alias_to_real"]),
            ),
            "counters": dict(original["counters"]),
            "updated_at": original.get("updated_at", ""),
        }

    @staticmethod
    def _materialize_delta(
        original: dict[str, Any],
        transaction: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the complete JSON document once, only for a real commit."""
        snapshot = dict(original)
        snapshot["real_to_alias"] = dict(transaction["real_to_alias"])
        snapshot["alias_to_real"] = dict(transaction["alias_to_real"])
        snapshot["counters"] = dict(transaction["counters"])
        snapshot["map_version"] = int(transaction.get("map_version", 1))
        return snapshot

    def _real_from_map(
        self,
        alias: str,
        amap: dict[str, Any],
        *,
        expected_type: str | None = None,
    ) -> str:
        raw = amap["alias_to_real"].get(alias)
        if not isinstance(raw, dict):
            raise KeyError(f"unknown alias={alias} in bucket={self.bucket_id}")
        key_type = str(raw.get("key_type", "")).strip().lower()
        real_id = str(raw.get("real_key", "")).strip()
        expected = _normalize_key_type(expected_type) if expected_type else None
        if expected and key_type != expected:
            raise TypeError(
                f"alias type mismatch: alias={alias}, expected={expected}, got={key_type}"
            )
        if key_type != KEY_TYPE_REF and infer_real_key_type(real_id) != key_type:
            raise ValueError(f"invalid mapped real key for alias={alias}")
        if key_type == KEY_TYPE_REF and not real_id:
            raise ValueError(f"invalid mapped real key for alias={alias}")
        return real_id

    def _decode_value(self, value: Any, amap: dict[str, Any], *, strict_unknown: bool) -> Any:
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for key, item in value.items():
                key_out = self._decode_value(str(key), amap, strict_unknown=strict_unknown)
                key_token = str(key_out)
                if key_token in out:
                    raise AliasPayloadError(f"real key collision after alias decode: {key_token}")
                out[key_token] = self._decode_value(item, amap, strict_unknown=strict_unknown)
            return out
        if isinstance(value, list):
            return [self._decode_value(item, amap, strict_unknown=strict_unknown) for item in value]
        if isinstance(value, tuple):
            return tuple(self._decode_value(item, amap, strict_unknown=strict_unknown) for item in value)
        if not isinstance(value, str):
            return value

        token = value.strip()
        if not looks_like_alias(token):
            return value
        reverse = amap["alias_to_real"].get(token)
        if not isinstance(reverse, dict):
            if strict_unknown:
                raise AliasPayloadError(f"unknown alias in bucket={self.bucket_id}: {token}")
            return value
        real_id = str(reverse.get("real_key", "")).strip()
        if not real_id:
            raise AliasPayloadError(f"invalid reverse alias mapping: {token}")
        return real_id

    @staticmethod
    def _assert_map_version(amap: dict[str, Any], map_version: int | None) -> None:
        if map_version is None:
            return
        current = int(amap.get("map_version", 1))
        if int(map_version) != current:
            raise AliasPayloadError(f"alias map version changed: expect={map_version}, got={current}")

    def _encode_with_callbacks(self, value: Any, *, allow_create: bool, forced_type: str | None = None) -> Any:
        if forced_type and isinstance(value, str):
            if allow_create:
                return self.storage.get_or_create_alias(self.bucket_id, value, forced_type)
            found = self.storage.find_alias(self.bucket_id, value, forced_type)
            if found:
                return found
            raise AliasPayloadError(f"missing alias for {forced_type}:{value}")
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for key, item in value.items():
                key_out = self._encode_with_callbacks(str(key), allow_create=allow_create)
                if key_out in out:
                    raise AliasPayloadError(f"alias key collision: {key_out}")
                out[str(key_out)] = self._encode_with_callbacks(item, allow_create=allow_create)
            return out
        if isinstance(value, list):
            return [self._encode_with_callbacks(item, allow_create=allow_create) for item in value]
        if isinstance(value, tuple):
            return tuple(self._encode_with_callbacks(item, allow_create=allow_create) for item in value)
        if not isinstance(value, str):
            return value

        def _replace(match: re.Match[str]) -> str:
            real_id = match.group(0)
            key_type = infer_real_key_type(real_id)
            if allow_create:
                return str(self.storage.get_or_create_alias(self.bucket_id, real_id, key_type))
            found = self.storage.find_alias(self.bucket_id, real_id, key_type)
            if found:
                return str(found)
            raise AliasPayloadError(f"missing alias for {key_type}:{real_id}")

        return _REAL_ID_IN_TEXT_RE.sub(_replace, value)

    def _encode_value(
        self,
        value: Any,
        amap: dict[str, Any],
        *,
        allow_create: bool,
        added: list[int],
        forced_type: str | None = None,
    ) -> Any:
        if forced_type and isinstance(value, str):
            return self._alias_from_map(value, forced_type, amap, allow_create=allow_create, added=added)
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for key, item in value.items():
                key_out = self._encode_value(str(key), amap, allow_create=allow_create, added=added)
                key_token = str(key_out)
                if key_token in out:
                    raise AliasPayloadError(f"alias key collision: {key_token}")
                out[key_token] = self._encode_value(item, amap, allow_create=allow_create, added=added)
            return out
        if isinstance(value, list):
            return [self._encode_value(item, amap, allow_create=allow_create, added=added) for item in value]
        if isinstance(value, tuple):
            return tuple(self._encode_value(item, amap, allow_create=allow_create, added=added) for item in value)
        if not isinstance(value, str):
            return value

        def _replace(match: re.Match[str]) -> str:
            real_id = match.group(0)
            key_type = infer_real_key_type(real_id)
            return self._alias_from_map(real_id, key_type, amap, allow_create=allow_create, added=added)

        return _REAL_ID_IN_TEXT_RE.sub(_replace, value)

    def _alias_from_map(
        self,
        real_id: str,
        key_type: str,
        amap: dict[str, Any],
        *,
        allow_create: bool,
        added: list[int],
    ) -> str:
        real_to_alias = amap["real_to_alias"]
        typed_key = f"{key_type}:{real_id}"
        existing = str(real_to_alias.get(typed_key, "")).strip()
        if existing:
            return existing
        if not allow_create:
            raise AliasPayloadError(f"missing alias for {typed_key}")
        if bool(amap.get("sealed", False)):
            raise RuntimeError(f"alias map sealed; cannot allocate new alias in bucket={self.bucket_id}")

        counters = amap["counters"]
        current = max(0, int(counters.get(key_type, 0))) + 1
        alias = f"{key_type}_{current}"
        alias_to_real = amap["alias_to_real"]
        if alias in alias_to_real:
            raise AliasPayloadError(f"alias counter collision: {alias}")
        counters[key_type] = current
        real_to_alias[typed_key] = alias
        alias_to_real[alias] = {"key_type": key_type, "real_key": real_id}
        amap["map_version"] = int(amap.get("map_version", 1)) + 1
        added[0] += 1
        return alias

    @staticmethod
    def _validate_map(amap: dict[str, Any]) -> None:
        validate_alias_map_payload(amap, _copy_payload=False)

    def _find_leak(self, value: Any, path: str) -> str:
        if isinstance(value, dict):
            for key, item in value.items():
                key_match = _REAL_ID_IN_TEXT_RE.search(str(key))
                if key_match:
                    return f"{path}.key={key_match.group(0)}"
                leak = self._find_leak(item, f"{path}.{key}")
                if leak:
                    return leak
            return ""
        if isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                leak = self._find_leak(item, f"{path}[{index}]")
                if leak:
                    return leak
            return ""
        if isinstance(value, str):
            match = _REAL_ID_IN_TEXT_RE.search(value)
            if match:
                return f"{path}={match.group(0)}"
        return ""


class AliasStore:
    """Owns one AliasTable instance per bucket for an engine storage."""

    def __init__(self, storage: Any) -> None:
        self.storage = storage
        self._tables: dict[str, AliasTable] = {}
        self._lock = threading.RLock()

    def open(self, bucket_id: str) -> AliasTable:
        token = str(bucket_id or "").strip()
        if not token:
            raise ValueError("bucket_id is empty")
        with self._lock:
            table = self._tables.get(token)
            if table is None:
                table = AliasTable(self.storage, token)
                self._tables[token] = table
            return table

    async def prepare(
        self,
        bucket_id: str,
        value: Any,
        *,
        allow_create: bool = True,
        map_version: int | None = None,
    ) -> Any:
        return await self.open(bucket_id).prepare(
            value,
            allow_create=allow_create,
            map_version=map_version,
        )

    async def restore(
        self,
        bucket_id: str,
        value: Any,
        *,
        map_version: int | None = None,
        strict_unknown: bool = True,
    ) -> Any:
        return await self.open(bucket_id).restore(
            value,
            map_version=map_version,
            strict_unknown=strict_unknown,
        )


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

    def __post_init__(self) -> None:
        self.store = AliasStore(self.storage)

    def get_or_create_alias(self, bucket_id: str, real_key: str, key_type: str) -> str:
        return self.store.open(bucket_id).to_alias(real_key, key_type=_normalize_key_type(key_type))

    def resolve_alias(self, bucket_id: str, alias: str, expected_type: str | None = None) -> str:
        expected = _normalize_key_type(expected_type) if expected_type else None
        return self.store.open(bucket_id).to_real(alias, expected_type=expected)

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
        table = self.store.open(bucket_id)
        generic_payload = table.encode_tree(real_payload, allow_create=allow_create)
        alias_payload = self._walk_to_alias(bucket_id, generic_payload, "", allow_create=allow_create)
        table.assert_safe(alias_payload)
        return alias_payload

    def resolve_llm_output(self, bucket_id: str, alias_output: Any, map_version: int | None = None) -> Any:
        if map_version is not None and int(map_version) != self.alias_map_version(bucket_id):
            raise AliasPayloadError(f"alias map version changed: expect={map_version}, got={self.alias_map_version(bucket_id)}")
        return self._walk_to_real(bucket_id, alias_output, "")

    def assert_alias_only_payload(self, bucket_id: str, payload: Any) -> None:
        self.store.open(bucket_id).assert_safe(payload)

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
                key_out = self._resolve_known_alias_key(bucket_id, sk)
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

    def _resolve_known_alias_key(self, bucket_id: str, value: str) -> str:
        token = str(value or "").strip()
        if not looks_like_alias(token):
            return value
        try:
            return self.resolve_alias(bucket_id, token, None)
        except KeyError:
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
