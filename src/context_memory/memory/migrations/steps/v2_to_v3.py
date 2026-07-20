from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...aliasing import AliasPayloadError, validate_alias_map_payload
from ...models import BucketInfo, utc_now_iso
from ..registry import register_step


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"invalid json file: {path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"json object required: {path}")
    return payload


def _resolve_successor(buckets: dict[str, Any], bucket_id: str) -> str:
    current = str(bucket_id or "").strip()
    visited: set[str] = set()
    while current:
        if current in visited:
            raise RuntimeError(f"bucket successor cycle: {current}")
        visited.add(current)
        raw = buckets.get(current)
        if not isinstance(raw, dict):
            return "" if len(visited) == 1 else _raise_dangling_successor(current)
        info = BucketInfo.from_dict(raw)
        if not info.sealed:
            return current
        successor = str(info.sealed_to or "").strip()
        if not successor:
            if info.archived:
                return ""
            raise RuntimeError(f"sealed bucket missing successor: {current}")
        current = successor
    return ""


def _raise_dangling_successor(bucket_id: str) -> str:
    raise RuntimeError(f"bucket successor not found: {bucket_id}")


def _validate_alias_relation(
    *,
    bucket_id: str,
    bucket_raw: dict[str, Any],
    alias_payload: dict[str, Any],
    buckets: dict[str, Any],
) -> None:
    info = BucketInfo.from_dict(bucket_raw)
    alias_sealed = bool(alias_payload.get("sealed", False))
    if info.sealed:
        if not alias_sealed:
            raise RuntimeError(f"sealed bucket has writable alias map: {bucket_id}")
        successor = str(info.sealed_to or "").strip()
        if successor:
            if not isinstance(buckets.get(successor), dict):
                raise RuntimeError(f"sealed bucket successor not found: {bucket_id} -> {successor}")
        elif not info.archived:
            raise RuntimeError(f"sealed bucket missing successor: {bucket_id}")
    elif alias_sealed:
        raise RuntimeError(f"active bucket has sealed alias map: {bucket_id}")


def _normalize_alias_maps(storage: Any, buckets: dict[str, Any]) -> dict[str, int]:
    stats = {"scanned": 0, "created": 0, "normalized": 0, "unchanged": 0}
    for bucket_id, bucket_raw in buckets.items():
        if not isinstance(bucket_raw, dict):
            raise RuntimeError(f"invalid bucket info: {bucket_id}")
        stats["scanned"] += 1
        path = Path(storage.buckets_dir) / str(bucket_id) / "alias_map.json"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            storage._atomic_save_json(storage._default_alias_map(str(bucket_id)), path)
            stats["created"] += 1
            payload = _read_json_object(path)
        else:
            payload = _read_json_object(path)

        try:
            normalized, changed = validate_alias_map_payload(
                payload,
                bucket_id=str(bucket_id),
                normalize_metadata=True,
            )
        except (AliasPayloadError, ValueError) as exc:
            raise RuntimeError(f"invalid alias map: {bucket_id}: {exc}") from exc

        _validate_alias_relation(
            bucket_id=str(bucket_id),
            bucket_raw=bucket_raw,
            alias_payload=normalized,
            buckets=buckets,
        )
        if changed:
            normalized["updated_at"] = utc_now_iso()
            storage._atomic_save_json(normalized, path)
            stats["normalized"] += 1
        else:
            stats["unchanged"] += 1
    return stats


class _V2ToV3Step:
    id = "v2_to_v3_parent_scoped_title_maps"
    from_version = 2
    to_version = 3

    def apply(self, *, storage: Any, context: Any) -> dict[str, Any]:
        tree = storage.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            raise RuntimeError("bucket tree buckets is invalid")

        title_maps: dict[str, dict[str, str]] = {}
        legacy_path = Path(storage.root_dir) / "bucket_mapping.json"
        legacy = _read_json_object(legacy_path) if legacy_path.exists() else {}
        migrated = 0
        stale = 0
        for title, raw_target in legacy.items():
            target = _resolve_successor(buckets, str(raw_target))
            if not target:
                stale += 1
                continue
            target_raw = buckets.get(target)
            if not isinstance(target_raw, dict):
                stale += 1
                continue
            child = BucketInfo.from_dict(target_raw)
            parent_id = str(child.parent_bucket_id or "").strip()
            if not parent_id:
                stale += 1
                continue
            parent_raw = buckets.get(parent_id)
            if not isinstance(parent_raw, dict):
                raise RuntimeError(f"mapped bucket parent not found: child={target}, parent={parent_id}")
            parent = BucketInfo.from_dict(parent_raw)
            if target not in parent.children or child.parent_bucket_id != parent_id:
                raise RuntimeError(f"mapped bucket topology mismatch: title={title}, child={target}, parent={parent_id}")
            parent_map = title_maps.setdefault(parent_id, {})
            existing = parent_map.get(str(title))
            if existing and existing != target:
                raise RuntimeError(f"duplicate migrated title mapping: parent={parent_id}, title={title}")
            parent_map[str(title)] = target
            migrated += 1

        tree["child_title_maps"] = title_maps
        storage.save_bucket_tree(tree)
        alias_stats = _normalize_alias_maps(storage, buckets)
        legacy_path.unlink(missing_ok=True)
        return {
            "title_mapping": {"legacy": len(legacy), "migrated": migrated, "stale": stale},
            "alias_maps": alias_stats,
        }

    def validate(self, *, storage: Any, context: Any) -> dict[str, Any]:
        tree = storage.load_bucket_tree()
        buckets = tree.get("buckets", {})
        title_maps = tree.get("child_title_maps", {})
        if not isinstance(buckets, dict) or not isinstance(title_maps, dict):
            raise RuntimeError("invalid v3 bucket tree")
        checked = 0
        for parent_id, parent_map in title_maps.items():
            parent_raw = buckets.get(parent_id)
            if not isinstance(parent_raw, dict) or not isinstance(parent_map, dict):
                raise RuntimeError(f"invalid child title map parent: {parent_id}")
            parent = BucketInfo.from_dict(parent_raw)
            for title, child_id in parent_map.items():
                child_raw = buckets.get(str(child_id))
                if not isinstance(child_raw, dict):
                    raise RuntimeError(f"child title target not found: parent={parent_id}, title={title}")
                child = BucketInfo.from_dict(child_raw)
                if child.parent_bucket_id != parent_id or str(child_id) not in parent.children:
                    raise RuntimeError(f"child title topology mismatch: parent={parent_id}, title={title}")
                checked += 1

        alias_stats = _normalize_alias_maps(storage, buckets)
        if (Path(storage.root_dir) / "bucket_mapping.json").exists():
            raise RuntimeError("legacy bucket_mapping.json remains after v3 migration")
        return {"title_mappings_checked": checked, "alias_maps": alias_stats}


register_step(_V2ToV3Step())
