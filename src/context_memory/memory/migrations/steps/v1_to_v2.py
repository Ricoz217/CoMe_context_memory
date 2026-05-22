from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ..registry import register_step

_EVENT_PREFIX = "[MEM_EVENT]"


def _parse_event_ts(raw: Any) -> float:
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return 0.0


def _iter_json_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("*.json") if p.is_file())


def _read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class _V1ToV2Step:
    id = "v1_to_v2_confidence_and_bucket_last_event"
    from_version = 1
    to_version = 2

    @staticmethod
    def _patch_memories(storage: Any) -> dict[str, Any]:
        touched = 0
        scanned = 0
        for path in _iter_json_files(Path(storage.memories_dir)):
            scanned += 1
            payload = _read_json_file(path)
            if payload is None:
                continue
            if str(payload.get("confidence_type", "")).strip():
                continue
            payload["confidence_type"] = "common"
            _write_json_file(path, payload)
            touched += 1
        return {"scanned": scanned, "patched": touched}

    @staticmethod
    def _patch_events_and_context(storage: Any) -> tuple[dict[str, Any], dict[str, Any], dict[str, float]]:
        context_patched = 0
        events_patched = 0
        context_scanned = 0
        events_scanned = 0
        own_bucket_ts: dict[str, float] = {}

        bucket_dirs = sorted([p for p in Path(storage.buckets_dir).iterdir() if p.is_dir()]) if Path(storage.buckets_dir).exists() else []
        for bdir in bucket_dirs:
            bucket_id = bdir.name
            latest_ts = 0.0

            events_path = bdir / "events.ndjson"
            if events_path.exists():
                out_lines: list[str] = []
                changed = False
                for raw in events_path.read_text(encoding="utf-8").splitlines():
                    line = raw.strip()
                    if not line:
                        continue
                    events_scanned += 1
                    try:
                        event = json.loads(line)
                    except Exception:
                        out_lines.append(line)
                        continue
                    if not isinstance(event, dict):
                        out_lines.append(line)
                        continue
                    latest_ts = max(latest_ts, _parse_event_ts(event.get("created_at")))
                    if not str(event.get("confidence_type", "")).strip():
                        event["confidence_type"] = "common"
                        changed = True
                        events_patched += 1
                    out_lines.append(json.dumps(event, ensure_ascii=False, sort_keys=True))
                if changed:
                    events_path.write_text("\n".join(out_lines) + ("\n" if out_lines else ""), encoding="utf-8")

            context_path = bdir / "context.json"
            payload = _read_json_file(context_path) if context_path.exists() else None
            if isinstance(payload, dict):
                messages = payload.get("messages", {})
                changed = False
                if isinstance(messages, dict):
                    for _, msg in messages.items():
                        if not isinstance(msg, dict):
                            continue
                        data = msg.get("data", {})
                        if not isinstance(data, dict):
                            continue
                        text = data.get("text")
                        if not isinstance(text, str):
                            continue
                        if not text.startswith(_EVENT_PREFIX):
                            continue
                        context_scanned += 1
                        raw_json = text[len(_EVENT_PREFIX) :]
                        try:
                            event = json.loads(raw_json)
                        except Exception:
                            continue
                        if not isinstance(event, dict):
                            continue
                        latest_ts = max(latest_ts, _parse_event_ts(event.get("created_at")))
                        if not str(event.get("confidence_type", "")).strip():
                            event["confidence_type"] = "common"
                            data["text"] = f"{_EVENT_PREFIX}{json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(',', ':'))}"
                            changed = True
                            context_patched += 1
                if changed:
                    _write_json_file(context_path, payload)

            if latest_ts > 0.0:
                own_bucket_ts[bucket_id] = latest_ts

        return (
            {"scanned": context_scanned, "patched": context_patched},
            {"scanned": events_scanned, "patched": events_patched},
            own_bucket_ts,
        )

    @staticmethod
    def _patch_state(storage: Any) -> dict[str, Any]:
        state = storage.load_state()
        keys = state.get("keys", {})
        if not isinstance(keys, dict):
            return {"scanned": 0, "patched": 0}
        patched = 0
        scanned = 0
        for key, node in keys.items():
            if not isinstance(node, dict):
                continue
            scanned += 1
            if str(node.get("confidence_type", "")).strip():
                continue
            confidence = "common"
            path_text = str(node.get("latest_path", "")).strip()
            if path_text:
                p = storage._resolve_root_path(path_text)
                rec = _read_json_file(p)
                if isinstance(rec, dict):
                    confidence = str(rec.get("confidence_type", "common") or "common")
            node["confidence_type"] = confidence
            keys[key] = node
            patched += 1
        if patched > 0:
            state["keys"] = keys
            storage.save_state(state)
        return {"scanned": scanned, "patched": patched}

    @staticmethod
    def _patch_bucket_tree(storage: Any, own_bucket_ts: dict[str, float]) -> dict[str, Any]:
        tree = storage.load_bucket_tree()
        buckets = tree.get("buckets", {})
        if not isinstance(buckets, dict):
            return {"scanned": 0, "patched": 0}

        memo: dict[str, float] = {}

        def _aggregate(bucket_id: str, stack: set[str]) -> float:
            if bucket_id in memo:
                return memo[bucket_id]
            if bucket_id in stack:
                return float(own_bucket_ts.get(bucket_id, 0.0))
            raw = buckets.get(bucket_id)
            if not isinstance(raw, dict):
                memo[bucket_id] = float(own_bucket_ts.get(bucket_id, 0.0))
                return memo[bucket_id]
            stack.add(bucket_id)
            out = float(own_bucket_ts.get(bucket_id, 0.0))
            children = raw.get("children", [])
            if isinstance(children, list):
                for child in children:
                    cid = str(child).strip()
                    if not cid:
                        continue
                    out = max(out, _aggregate(cid, stack))
            stack.discard(bucket_id)
            memo[bucket_id] = out
            return out

        scanned = 0
        patched = 0
        for bucket_id, raw in buckets.items():
            if not isinstance(raw, dict):
                continue
            scanned += 1
            final_ts = _aggregate(str(bucket_id), set())
            prev = float(raw.get("last_event_at", 0.0) or 0.0)
            if abs(prev - final_ts) > 1e-9:
                raw["last_event_at"] = final_ts
                raw["updated_at"] = str(raw.get("updated_at", "") or raw.get("created_at", ""))
                buckets[bucket_id] = raw
                patched += 1
        if patched > 0:
            tree["buckets"] = buckets
            storage.save_bucket_tree(tree)
        return {"scanned": scanned, "patched": patched}

    def apply(self, *, storage: Any, context: Any) -> dict[str, Any]:
        memory_stats = self._patch_memories(storage)
        context_stats, events_stats, own_bucket_ts = self._patch_events_and_context(storage)
        state_stats = self._patch_state(storage)
        bucket_stats = self._patch_bucket_tree(storage, own_bucket_ts)
        return {
            "memory_files": memory_stats,
            "context_events": context_stats,
            "events_ndjson": events_stats,
            "state_keys": state_stats,
            "bucket_tree": bucket_stats,
        }

    def validate(self, *, storage: Any, context: Any) -> dict[str, Any]:
        memory_missing = 0
        memory_total = 0
        for path in _iter_json_files(Path(storage.memories_dir)):
            payload = _read_json_file(path)
            if payload is None:
                continue
            memory_total += 1
            if not str(payload.get("confidence_type", "")).strip():
                memory_missing += 1

        tree = storage.load_bucket_tree()
        buckets = tree.get("buckets", {})
        bucket_missing = 0
        bucket_total = 0
        if isinstance(buckets, dict):
            for _, raw in buckets.items():
                if not isinstance(raw, dict):
                    continue
                bucket_total += 1
                try:
                    float(raw.get("last_event_at", 0.0) or 0.0)
                except Exception:
                    bucket_missing += 1

        if memory_missing > 0 or bucket_missing > 0:
            raise RuntimeError(
                f"v1_to_v2 validate failed: "
                f"memory_missing={memory_missing}/{memory_total}, "
                f"bucket_missing={bucket_missing}/{bucket_total}"
            )

        return {
            "memory_total": memory_total,
            "memory_missing": memory_missing,
            "bucket_total": bucket_total,
            "bucket_missing": bucket_missing,
        }


register_step(_V1ToV2Step())

