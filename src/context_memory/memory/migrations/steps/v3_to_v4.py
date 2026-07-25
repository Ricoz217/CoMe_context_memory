from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...index_repository import IndexRepository
from ..registry import register_step


_LEGACY_INDEX_NAMES = ("state.json", "bucket_tree.json", "meta.json", "query_cache.json")


def _remove_sqlite_artifacts(path: Path) -> None:
    path.unlink(missing_ok=True)
    path.with_name(f"{path.name}-wal").unlink(missing_ok=True)
    path.with_name(f"{path.name}-shm").unlink(missing_ok=True)


def _validate_latest_revisions(storage: Any, state: dict[str, Any]) -> int:
    keys = state.get("keys", {})
    if not isinstance(keys, dict):
        raise RuntimeError("legacy state keys is invalid")
    checked = 0
    for key, node in keys.items():
        if not isinstance(node, dict):
            raise RuntimeError(f"invalid state node: {key}")
        path_text = str(node.get("latest_path", "")).strip()
        path = storage._resolve_root_path(path_text)
        if not path.is_file():
            raise RuntimeError(f"revision file not found: {key}: {path_text}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"invalid revision json: {key}: {path_text}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"invalid revision payload: {key}: {path_text}")
        expected = {
            "key": str(key),
            "revision_id": str(node.get("latest_revision", "")),
            "bucket_id": str(node.get("bucket_id", "")),
        }
        for field, value in expected.items():
            if str(payload.get(field, "")) != value:
                raise RuntimeError(
                    f"revision index mismatch: {key}: field={field}, "
                    f"index={value!r}, revision={payload.get(field)!r}"
                )
        checked += 1
    return checked


def _normalized_state(value: dict[str, Any]) -> dict[str, Any]:
    keys = value.get("keys", {})
    normalized_keys: dict[str, dict[str, Any]] = {}
    if isinstance(keys, dict):
        for key, raw in keys.items():
            if not isinstance(raw, dict):
                continue
            node = dict(raw)
            node.setdefault("confidence_type", "common")
            node.setdefault("child_bucket_id", "")
            node.setdefault("gray", False)
            node.setdefault("expires_at", None)
            node.setdefault("created_at", "")
            node.setdefault("updated_at", "")
            node.setdefault("revision_count", 0)
            node.setdefault("latest_evidence_ref", "")
            node.setdefault("evidence_history", [])
            node.setdefault("query_hits", 0)
            node.setdefault("last_recalled_at", "")
            node.setdefault("last_compress_penalty_at", "")
            node.setdefault("last_negative_weight", 0.0)
            normalized_keys[str(key)] = node
    return {
        "keys": normalized_keys,
        "revision_total": int(value.get("revision_total", 0)),
    }


def _normalized_tree(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "root_bucket_id": str(value.get("root_bucket_id", "")),
        "active_bucket_id": str(value.get("active_bucket_id", "")),
        "buckets": value.get("buckets", {}),
        "child_title_maps": value.get("child_title_maps", {}),
    }


def _normalized_meta(value: dict[str, Any]) -> dict[str, Any]:
    out = dict(value)
    out.setdefault("bucket_versions", {})
    out.setdefault("auto_split_last_at_by_bucket", {})
    return out


class _V3ToV4Step:
    id = "v3_to_v4_sqlite_index"
    from_version = 3
    to_version = 4

    def apply(self, *, storage: Any, context: Any) -> dict[str, Any]:
        state = storage.load_state()
        tree = storage.load_bucket_tree()
        meta = storage.load_meta()
        if Path(storage.events_file).exists():
            event_total = sum(
                1
                for line in Path(storage.events_file).read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
        else:
            event_total = 0
        meta = dict(meta)
        meta["event_total"] = event_total
        checked = _validate_latest_revisions(storage, state)
        database = Path(storage.sqlite_index_file)
        _remove_sqlite_artifacts(database)
        repo: IndexRepository | None = None
        try:
            repo = IndexRepository.create_from_legacy(
                database,
                state=state,
                tree=tree,
                meta=meta,
            )
            if _normalized_state(repo.load_state_snapshot()) != _normalized_state(state):
                raise RuntimeError("SQLite state snapshot differs from legacy state")
            if _normalized_tree(repo.load_tree_snapshot()) != _normalized_tree(tree):
                raise RuntimeError("SQLite topology snapshot differs from legacy tree")
            if _normalized_meta(repo.load_meta()) != _normalized_meta(meta):
                raise RuntimeError("SQLite meta snapshot differs from legacy meta")
            checks = repo.integrity_check()
            if checks["integrity_check"] != "ok" or checks["foreign_key_errors"]:
                raise RuntimeError(f"SQLite integrity validation failed: {checks}")
            repo.clear_query_cache()
            repo.checkpoint()
            repo.close()
            repo = None
            for name in _LEGACY_INDEX_NAMES:
                (Path(storage.index_dir) / name).unlink(missing_ok=True)
            return {
                "records_imported": len(state.get("keys", {})),
                "buckets_imported": len(tree.get("buckets", {})),
                "latest_revisions_checked": checked,
                "events_imported": event_total,
                "query_cache_cleared": True,
            }
        except Exception:
            if repo is not None:
                repo.close()
            _remove_sqlite_artifacts(database)
            raise

    def validate(self, *, storage: Any, context: Any) -> dict[str, Any]:
        database = Path(storage.sqlite_index_file)
        if not database.is_file():
            raise RuntimeError("schema v4 SQLite index is missing")
        remaining = [
            name for name in _LEGACY_INDEX_NAMES if (Path(storage.index_dir) / name).exists()
        ]
        if remaining:
            raise RuntimeError(f"legacy index files remain after v4 migration: {remaining}")
        repo = IndexRepository(database)
        try:
            checks = repo.integrity_check()
            if checks["integrity_check"] != "ok" or checks["foreign_key_errors"]:
                raise RuntimeError(f"SQLite integrity validation failed: {checks}")
            return {
                "integrity_check": checks["integrity_check"],
                "foreign_key_errors": checks["foreign_key_errors"],
                "records": repo.index_diagnostics()["locator_count"],
                "buckets": repo.index_diagnostics()["bucket_count"],
            }
        finally:
            repo.close()


register_step(_V3ToV4Step())
