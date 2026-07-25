from __future__ import annotations

import json
import sqlite3
import sys
import threading
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping
from uuid import uuid4

from .models import BucketInfo, utc_now_iso


SCHEMA_VERSION = 4
SQLITE_FILENAME = "memory_index.sqlite3"


class RepositoryError(RuntimeError):
    pass


class RepositoryWriteError(RepositoryError):
    pass


@dataclass(slots=True, frozen=True)
class RecordLocator:
    key: str
    latest_revision: str
    latest_path: str
    bucket_id: str
    kind: Literal["memory", "bucket"]
    child_bucket_id: str
    gray: bool
    expires_at: str | None
    updated_at: str


@dataclass(slots=True, frozen=True)
class _IndexView:
    locators: dict[str, RecordLocator]
    memory_keys_by_bucket: dict[str, tuple[str, ...]]
    bucket_node_keys_by_bucket: dict[str, tuple[str, ...]]
    buckets: dict[str, BucketInfo]
    children_by_parent: dict[str, tuple[str, ...]]
    title_targets: dict[tuple[str, str], str]
    bucket_versions: dict[str, int]
    root_bucket_id: str
    active_bucket_id: str
    topology_updated_at: str


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _new_bucket_id() -> str:
    return f"bucket_{_utc_stamp()}_{uuid4().hex}"


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_load(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


class IndexRepository:
    """SQLite index plus a compact immutable in-process read view."""

    _RECORD_UPSERT_SQL = """
        INSERT INTO records(
            key, latest_revision, latest_path, bucket_id, kind, child_bucket_id,
            confidence_type, gray, expires_at, created_at, updated_at, revision_count,
            latest_evidence_ref, evidence_history_json, query_hits, last_recalled_at,
            last_compress_penalty_at, last_negative_weight
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            latest_revision=excluded.latest_revision,
            latest_path=excluded.latest_path,
            bucket_id=excluded.bucket_id,
            kind=excluded.kind,
            child_bucket_id=excluded.child_bucket_id,
            confidence_type=excluded.confidence_type,
            gray=excluded.gray,
            expires_at=excluded.expires_at,
            updated_at=excluded.updated_at,
            revision_count=excluded.revision_count,
            latest_evidence_ref=excluded.latest_evidence_ref,
            evidence_history_json=excluded.evidence_history_json,
            query_hits=excluded.query_hits,
            last_recalled_at=excluded.last_recalled_at,
            last_compress_penalty_at=excluded.last_compress_penalty_at,
            last_negative_weight=excluded.last_negative_weight
    """

    def __init__(self, path: str | Path, *, create: bool = False) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not create and not self.path.exists():
            raise RepositoryError(f"index database not found: {self.path}")
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            self.path,
            timeout=5.0,
            check_same_thread=False,
            isolation_level=None,
        )
        self._connection.row_factory = sqlite3.Row
        self._configure()
        if create:
            self._create_schema()
        self._validate_schema_version()
        self._view = self._build_view()

    @classmethod
    def create_empty(cls, path: str | Path, *, prompt_version: str = "v3") -> "IndexRepository":
        target = Path(path)
        if target.exists():
            target.unlink()
        repo = cls(target, create=True)
        root_id = _new_bucket_id()
        root = BucketInfo(
            bucket_id=root_id,
            parent_bucket_id=None,
            level=1,
            title="ROOT",
            summary="root bucket",
            node_key="",
        )
        now = utc_now_iso()
        try:
            repo._begin()
            repo._insert_bucket(root)
            repo._connection.execute(
                "INSERT INTO topology_meta(singleton, root_bucket_id, active_bucket_id, updated_at) "
                "VALUES (1, ?, ?, ?)",
                (root_id, root_id, now),
            )
            defaults = {
                "dirty": False,
                "context_version": 0,
                "prompt_version": prompt_version,
                "updated_at": now,
                "last_snapshot": "",
                "event_total": 0,
            }
            for key, value in defaults.items():
                repo._set_engine_meta_sql(key, value)
            repo._connection.execute(
                "UPDATE repository_meta SET created_at=?, updated_at=?, revision_total=0, prompt_version=? "
                "WHERE singleton=1",
                (now, now, prompt_version),
            )
            repo._commit()
        except Exception as exc:
            repo._rollback()
            repo.close()
            raise RepositoryWriteError(f"failed to create empty index: {exc}") from exc
        repo._view = repo._build_view()
        return repo

    @classmethod
    def create_from_legacy(
        cls,
        path: str | Path,
        *,
        state: dict[str, Any],
        tree: dict[str, Any],
        meta: dict[str, Any],
    ) -> "IndexRepository":
        target = Path(path)
        if target.exists():
            target.unlink()
        repo = cls(target, create=True)
        try:
            repo._begin()
            repo._import_legacy_sql(state=state, tree=tree, meta=meta)
            repo._commit()
        except Exception as exc:
            repo._rollback()
            repo.close()
            target.unlink(missing_ok=True)
            raise RepositoryWriteError(f"failed to import legacy index: {exc}") from exc
        repo._view = repo._build_view()
        return repo

    def _configure(self) -> None:
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=5000")

    def _create_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                key TEXT PRIMARY KEY,
                latest_revision TEXT NOT NULL,
                latest_path TEXT NOT NULL,
                bucket_id TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('memory', 'bucket')),
                child_bucket_id TEXT NOT NULL DEFAULT '',
                confidence_type TEXT NOT NULL DEFAULT 'common',
                gray INTEGER NOT NULL DEFAULT 0,
                expires_at TEXT,
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT '',
                revision_count INTEGER NOT NULL DEFAULT 0,
                latest_evidence_ref TEXT NOT NULL DEFAULT '',
                evidence_history_json TEXT NOT NULL DEFAULT '[]',
                query_hits INTEGER NOT NULL DEFAULT 0,
                last_recalled_at TEXT NOT NULL DEFAULT '',
                last_compress_penalty_at TEXT NOT NULL DEFAULT '',
                last_negative_weight REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_records_bucket_kind_gray
                ON records(bucket_id, kind, gray);
            CREATE INDEX IF NOT EXISTS idx_records_child_bucket
                ON records(child_bucket_id);
            CREATE INDEX IF NOT EXISTS idx_records_expires_gray
                ON records(expires_at, gray);

            CREATE TABLE IF NOT EXISTS buckets (
                bucket_id TEXT PRIMARY KEY,
                parent_bucket_id TEXT,
                level INTEGER NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                node_key TEXT NOT NULL,
                summary_status TEXT NOT NULL,
                summary_locked INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_event_at REAL NOT NULL DEFAULT 0,
                sealed INTEGER NOT NULL DEFAULT 0,
                sealed_to TEXT NOT NULL DEFAULT '',
                archived INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_buckets_parent ON buckets(parent_bucket_id);
            CREATE INDEX IF NOT EXISTS idx_buckets_sealed_to ON buckets(sealed_to);

            CREATE TABLE IF NOT EXISTS bucket_edges (
                parent_bucket_id TEXT NOT NULL,
                child_bucket_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                PRIMARY KEY(parent_bucket_id, child_bucket_id),
                UNIQUE(parent_bucket_id, ordinal),
                FOREIGN KEY(parent_bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE,
                FOREIGN KEY(child_bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS child_title_maps (
                parent_bucket_id TEXT NOT NULL,
                title TEXT NOT NULL,
                child_bucket_id TEXT NOT NULL,
                PRIMARY KEY(parent_bucket_id, title),
                FOREIGN KEY(parent_bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE,
                FOREIGN KEY(child_bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS topology_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                root_bucket_id TEXT NOT NULL,
                active_bucket_id TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(root_bucket_id) REFERENCES buckets(bucket_id),
                FOREIGN KEY(active_bucket_id) REFERENCES buckets(bucket_id)
            );
            CREATE TABLE IF NOT EXISTS engine_meta (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bucket_versions (
                bucket_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS auto_split_state (
                bucket_id TEXT PRIMARY KEY,
                last_at TEXT NOT NULL,
                FOREIGN KEY(bucket_id) REFERENCES buckets(bucket_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS query_cache (
                cache_key TEXT PRIMARY KEY,
                created_seq INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                bucket_id TEXT NOT NULL,
                bucket_version INTEGER NOT NULL,
                result_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_query_cache_created_seq
                ON query_cache(created_seq);
            CREATE TABLE IF NOT EXISTS repository_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                revision_total INTEGER NOT NULL DEFAULT 0,
                prompt_version TEXT NOT NULL
            );
            """
        )
        now = utc_now_iso()
        self._connection.execute(
            "INSERT OR IGNORE INTO repository_meta("
            "singleton, schema_version, created_at, updated_at, revision_total, prompt_version"
            ") VALUES (1, ?, ?, ?, 0, 'v3')",
            (SCHEMA_VERSION, now, now),
        )
        self._connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def _validate_schema_version(self) -> None:
        user_version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        row = self._connection.execute(
            "SELECT schema_version FROM repository_meta WHERE singleton=1"
        ).fetchone()
        repo_version = int(row[0]) if row is not None else 0
        if user_version != SCHEMA_VERSION or repo_version != SCHEMA_VERSION:
            raise RepositoryError(
                f"SQLite schema mismatch: pragma={user_version}, repository={repo_version}, code={SCHEMA_VERSION}"
            )

    def _import_legacy_sql(
        self,
        *,
        state: dict[str, Any],
        tree: dict[str, Any],
        meta: dict[str, Any],
    ) -> None:
        buckets_raw = tree.get("buckets", {})
        if not isinstance(buckets_raw, dict) or not buckets_raw:
            raise RepositoryWriteError("legacy bucket tree has no buckets")
        buckets: dict[str, BucketInfo] = {}
        for bucket_id, raw in buckets_raw.items():
            if not isinstance(raw, dict):
                raise RepositoryWriteError(f"invalid bucket entry: {bucket_id}")
            info = BucketInfo.from_dict(raw)
            if info.bucket_id != str(bucket_id):
                raise RepositoryWriteError(f"bucket id mismatch: {bucket_id}")
            buckets[info.bucket_id] = info
            self._insert_bucket(info)
        for info in buckets.values():
            for ordinal, child_id in enumerate(info.children):
                child = buckets.get(child_id)
                if child is None or child.parent_bucket_id != info.bucket_id:
                    raise RepositoryWriteError(
                        f"bucket topology mismatch: {info.bucket_id} -> {child_id}"
                    )
                self._connection.execute(
                    "INSERT INTO bucket_edges(parent_bucket_id, child_bucket_id, ordinal) VALUES (?, ?, ?)",
                    (info.bucket_id, child_id, ordinal),
                )
        title_maps = tree.get("child_title_maps", {})
        if not isinstance(title_maps, dict):
            raise RepositoryWriteError("invalid child_title_maps")
        for parent_id, parent_map in title_maps.items():
            if not isinstance(parent_map, dict):
                raise RepositoryWriteError(f"invalid title map: {parent_id}")
            for title, child_id in parent_map.items():
                parent = buckets.get(str(parent_id))
                child = buckets.get(str(child_id))
                if parent is None or child is None or child.parent_bucket_id != parent.bucket_id:
                    raise RepositoryWriteError(
                        f"title map topology mismatch: {parent_id}/{title} -> {child_id}"
                    )
                self._connection.execute(
                    "INSERT INTO child_title_maps(parent_bucket_id, title, child_bucket_id) VALUES (?, ?, ?)",
                    (str(parent_id), str(title), str(child_id)),
                )

        root_id = str(tree.get("root_bucket_id", "")).strip()
        active_id = str(tree.get("active_bucket_id", "")).strip() or root_id
        if root_id not in buckets or active_id not in buckets:
            raise RepositoryWriteError("invalid root or active bucket")
        self._connection.execute(
            "INSERT INTO topology_meta(singleton, root_bucket_id, active_bucket_id, updated_at) "
            "VALUES (1, ?, ?, ?)",
            (root_id, active_id, str(tree.get("updated_at", utc_now_iso()))),
        )

        keys = state.get("keys", {})
        if not isinstance(keys, dict):
            raise RepositoryWriteError("legacy state keys is invalid")
        for key, node in keys.items():
            if not isinstance(node, dict):
                raise RepositoryWriteError(f"invalid state node: {key}")
            self._upsert_record_sql(self._locator_from_node(str(key), node), node)

        bucket_versions = meta.get("bucket_versions", {})
        if isinstance(bucket_versions, dict):
            for bucket_id, version in bucket_versions.items():
                if str(bucket_id) in buckets:
                    self._connection.execute(
                        "INSERT INTO bucket_versions(bucket_id, version) VALUES (?, ?)",
                        (str(bucket_id), int(version)),
                    )
        split_state = meta.get("auto_split_last_at_by_bucket", {})
        if isinstance(split_state, dict):
            for bucket_id, last_at in split_state.items():
                if str(bucket_id) in buckets:
                    self._connection.execute(
                        "INSERT INTO auto_split_state(bucket_id, last_at) VALUES (?, ?)",
                        (str(bucket_id), str(last_at)),
                    )
        excluded = {"bucket_versions", "auto_split_last_at_by_bucket"}
        for key, value in meta.items():
            if key not in excluded:
                self._set_engine_meta_sql(str(key), value)
        self._connection.execute(
            "UPDATE repository_meta SET updated_at=?, revision_total=?, prompt_version=? WHERE singleton=1",
            (
                utc_now_iso(),
                int(state.get("revision_total", 0)),
                str(meta.get("prompt_version", "v3")),
            ),
        )

    @staticmethod
    def _locator_from_node(key: str, node: Mapping[str, Any]) -> RecordLocator:
        kind = str(node.get("kind", "memory"))
        if kind not in {"memory", "bucket"}:
            raise RepositoryWriteError(f"invalid record kind: {key}: {kind}")
        return RecordLocator(
            key=key,
            latest_revision=str(node.get("latest_revision", "")),
            latest_path=str(node.get("latest_path", "")),
            bucket_id=str(node.get("bucket_id", "")),
            kind=kind,  # type: ignore[arg-type]
            child_bucket_id=str(node.get("child_bucket_id", "")),
            gray=bool(node.get("gray", False)),
            expires_at=node.get("expires_at"),
            updated_at=str(node.get("updated_at", "")),
        )

    def _insert_bucket(self, info: BucketInfo) -> None:
        self._connection.execute(
            """
            INSERT INTO buckets(
                bucket_id, parent_bucket_id, level, title, summary, node_key,
                summary_status, summary_locked, created_at, updated_at,
                last_event_at, sealed, sealed_to, archived
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                info.bucket_id,
                info.parent_bucket_id,
                int(info.level),
                info.title,
                info.summary,
                info.node_key,
                info.summary_status,
                int(info.summary_locked),
                info.created_at,
                info.updated_at,
                float(info.last_event_at),
                int(info.sealed),
                info.sealed_to,
                int(info.archived),
            ),
        )

    def _upsert_record_sql(self, locator: RecordLocator, node: Mapping[str, Any]) -> None:
        self._connection.execute(self._RECORD_UPSERT_SQL, self._record_upsert_parameters(locator, node))

    @staticmethod
    def _record_upsert_parameters(locator: RecordLocator, node: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            locator.key,
            locator.latest_revision,
            locator.latest_path,
            locator.bucket_id,
            locator.kind,
            locator.child_bucket_id,
            str(node.get("confidence_type", "common") or "common"),
            int(locator.gray),
            locator.expires_at,
            str(node.get("created_at", "")),
            locator.updated_at,
            int(node.get("revision_count", 0)),
            str(node.get("latest_evidence_ref", "")),
            _json_dump(node.get("evidence_history", [])),
            int(node.get("query_hits", 0)),
            str(node.get("last_recalled_at", "")),
            str(node.get("last_compress_penalty_at", "")),
            float(node.get("last_negative_weight", 0.0) or 0.0),
        )

    def _build_view(self) -> _IndexView:
        locators: dict[str, RecordLocator] = {}
        memory_keys: dict[str, list[str]] = {}
        bucket_node_keys: dict[str, list[str]] = {}
        for row in self._connection.execute(
            "SELECT key, latest_revision, latest_path, bucket_id, kind, child_bucket_id, "
            "gray, expires_at, updated_at FROM records ORDER BY rowid"
        ):
            locator = RecordLocator(
                key=str(row["key"]),
                latest_revision=str(row["latest_revision"]),
                latest_path=str(row["latest_path"]),
                bucket_id=str(row["bucket_id"]),
                kind=str(row["kind"]),  # type: ignore[arg-type]
                child_bucket_id=str(row["child_bucket_id"]),
                gray=bool(row["gray"]),
                expires_at=row["expires_at"],
                updated_at=str(row["updated_at"]),
            )
            locators[locator.key] = locator
            target = memory_keys if locator.kind == "memory" else bucket_node_keys
            target.setdefault(locator.bucket_id, []).append(locator.key)

        bucket_rows = self._connection.execute("SELECT * FROM buckets").fetchall()
        buckets: dict[str, BucketInfo] = {}
        child_lists: dict[str, list[str]] = {}
        for row in self._connection.execute(
            "SELECT parent_bucket_id, child_bucket_id FROM bucket_edges ORDER BY parent_bucket_id, ordinal"
        ):
            child_lists.setdefault(str(row["parent_bucket_id"]), []).append(str(row["child_bucket_id"]))
        for row in bucket_rows:
            bucket_id = str(row["bucket_id"])
            buckets[bucket_id] = BucketInfo(
                bucket_id=bucket_id,
                parent_bucket_id=row["parent_bucket_id"],
                level=int(row["level"]),
                title=str(row["title"]),
                summary=str(row["summary"]),
                node_key=str(row["node_key"]),
                summary_status=str(row["summary_status"]),
                summary_locked=bool(row["summary_locked"]),
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
                children=list(child_lists.get(bucket_id, [])),
                sealed=bool(row["sealed"]),
                sealed_to=str(row["sealed_to"]),
                archived=bool(row["archived"]),
                last_event_at=float(row["last_event_at"]),
            )
        title_targets = {
            (str(row["parent_bucket_id"]), str(row["title"])): str(row["child_bucket_id"])
            for row in self._connection.execute(
                "SELECT parent_bucket_id, title, child_bucket_id FROM child_title_maps"
            )
        }
        versions = {
            str(row["bucket_id"]): int(row["version"])
            for row in self._connection.execute("SELECT bucket_id, version FROM bucket_versions")
        }
        topology = self._connection.execute(
            "SELECT root_bucket_id, active_bucket_id, updated_at FROM topology_meta WHERE singleton=1"
        ).fetchone()
        root_id = str(topology["root_bucket_id"]) if topology is not None else ""
        active_id = str(topology["active_bucket_id"]) if topology is not None else root_id
        topology_updated_at = str(topology["updated_at"]) if topology is not None else ""
        return _IndexView(
            locators=locators,
            memory_keys_by_bucket={k: tuple(v) for k, v in memory_keys.items()},
            bucket_node_keys_by_bucket={k: tuple(v) for k, v in bucket_node_keys.items()},
            buckets=buckets,
            children_by_parent={k: tuple(v) for k, v in child_lists.items()},
            title_targets=title_targets,
            bucket_versions=versions,
            root_bucket_id=root_id,
            active_bucket_id=active_id,
            topology_updated_at=topology_updated_at,
        )

    def _publish_view(self) -> None:
        replacement = self._build_view()
        with self._lock:
            self._view = replacement

    @staticmethod
    def _locator_from_row(row: sqlite3.Row) -> RecordLocator:
        return RecordLocator(
            key=str(row["key"]),
            latest_revision=str(row["latest_revision"]),
            latest_path=str(row["latest_path"]),
            bucket_id=str(row["bucket_id"]),
            kind=str(row["kind"]),  # type: ignore[arg-type]
            child_bucket_id=str(row["child_bucket_id"]),
            gray=bool(row["gray"]),
            expires_at=row["expires_at"],
            updated_at=str(row["updated_at"]),
        )

    def _publish_record_deltas(
        self,
        locators: Iterable[RecordLocator],
        *,
        deleted_keys: Iterable[str] = (),
        increment_bucket_versions: Iterable[str] = (),
    ) -> None:
        replacements = {locator.key: locator for locator in locators}
        deleted = set(str(key) for key in deleted_keys)
        with self._lock:
            view = self._view
            affected_keys = set(replacements) | deleted
            grouped_keys: dict[tuple[str, str], list[str]] = {}
            grouped_members: dict[tuple[str, str], set[str]] = {}
            removals: dict[tuple[str, str], set[str]] = {}

            for key in affected_keys:
                old = view.locators.get(key)
                if old is not None:
                    removals.setdefault((old.kind, old.bucket_id), set()).add(key)

            def bucket_group(kind: str, bucket_id: str) -> list[str]:
                group_id = (kind, bucket_id)
                if group_id not in grouped_keys:
                    source = (
                        view.memory_keys_by_bucket
                        if kind == "memory"
                        else view.bucket_node_keys_by_bucket
                    )
                    removed = removals.get(group_id, set())
                    grouped_keys[group_id] = [
                        key for key in source.get(bucket_id, ()) if key not in removed
                    ]
                    grouped_members[group_id] = set(grouped_keys[group_id])
                return grouped_keys[group_id]

            for key in affected_keys:
                old = view.locators.get(key)
                if old is None:
                    continue
                bucket_group(old.kind, old.bucket_id)
                view.locators.pop(key, None)

            for key, locator in replacements.items():
                view.locators[key] = locator
                group = bucket_group(locator.kind, locator.bucket_id)
                members = grouped_members[(locator.kind, locator.bucket_id)]
                if key not in members:
                    group.append(key)
                    members.add(key)

            for (kind, bucket_id), keys in grouped_keys.items():
                index = (
                    view.memory_keys_by_bucket
                    if kind == "memory"
                    else view.bucket_node_keys_by_bucket
                )
                if keys:
                    index[bucket_id] = tuple(keys)
                else:
                    index.pop(bucket_id, None)

            for bucket_id in increment_bucket_versions:
                view.bucket_versions[bucket_id] = int(view.bucket_versions.get(bucket_id, 0)) + 1

    def _load_topology_components(
        self,
    ) -> tuple[
        dict[str, BucketInfo],
        dict[str, tuple[str, ...]],
        dict[tuple[str, str], str],
        dict[str, int],
        str,
        str,
        str,
    ]:
        child_lists: dict[str, list[str]] = {}
        for row in self._connection.execute(
            "SELECT parent_bucket_id, child_bucket_id FROM bucket_edges ORDER BY parent_bucket_id, ordinal"
        ):
            child_lists.setdefault(str(row["parent_bucket_id"]), []).append(str(row["child_bucket_id"]))
        buckets: dict[str, BucketInfo] = {}
        for row in self._connection.execute("SELECT * FROM buckets"):
            bucket_id = str(row["bucket_id"])
            buckets[bucket_id] = BucketInfo(
                bucket_id=bucket_id,
                parent_bucket_id=row["parent_bucket_id"],
                level=int(row["level"]),
                title=str(row["title"]),
                summary=str(row["summary"]),
                node_key=str(row["node_key"]),
                summary_status=str(row["summary_status"]),
                summary_locked=bool(row["summary_locked"]),
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
                children=list(child_lists.get(bucket_id, [])),
                sealed=bool(row["sealed"]),
                sealed_to=str(row["sealed_to"]),
                archived=bool(row["archived"]),
                last_event_at=float(row["last_event_at"]),
            )
        title_targets = {
            (str(row["parent_bucket_id"]), str(row["title"])): str(row["child_bucket_id"])
            for row in self._connection.execute(
                "SELECT parent_bucket_id, title, child_bucket_id FROM child_title_maps"
            )
        }
        versions = {
            str(row["bucket_id"]): int(row["version"])
            for row in self._connection.execute("SELECT bucket_id, version FROM bucket_versions")
        }
        topology = self._connection.execute(
            "SELECT root_bucket_id, active_bucket_id, updated_at FROM topology_meta WHERE singleton=1"
        ).fetchone()
        root_id = str(topology["root_bucket_id"]) if topology is not None else ""
        active_id = str(topology["active_bucket_id"]) if topology is not None else root_id
        updated_at = str(topology["updated_at"]) if topology is not None else ""
        return (
            buckets,
            {key: tuple(value) for key, value in child_lists.items()},
            title_targets,
            versions,
            root_id,
            active_id,
            updated_at,
        )

    def _publish_topology_view(self) -> None:
        (
            buckets,
            children,
            title_targets,
            versions,
            root_id,
            active_id,
            updated_at,
        ) = self._load_topology_components()
        with self._lock:
            current = self._view
            self._view = _IndexView(
                locators=current.locators,
                memory_keys_by_bucket=current.memory_keys_by_bucket,
                bucket_node_keys_by_bucket=current.bucket_node_keys_by_bucket,
                buckets=buckets,
                children_by_parent=children,
                title_targets=title_targets,
                bucket_versions=versions,
                root_bucket_id=root_id,
                active_bucket_id=active_id,
                topology_updated_at=updated_at,
            )

    def _begin(self) -> None:
        self._connection.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        self._connection.commit()

    def _rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        with self._lock:
            if self._connection is None:
                return
            try:
                self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                self._connection.close()
                self._connection = None  # type: ignore[assignment]

    def checkpoint(self) -> None:
        with self._lock:
            self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def backup_to(self, target: str | Path) -> None:
        target_path = Path(target)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        destination = sqlite3.connect(target_path)
        try:
            with self._lock:
                self._connection.backup(destination)
        finally:
            destination.close()

    def root_bucket_id(self) -> str:
        with self._lock:
            return self._view.root_bucket_id

    def active_bucket_id(self) -> str:
        with self._lock:
            return self._view.active_bucket_id

    def get_locator(self, key: str) -> RecordLocator | None:
        with self._lock:
            return self._view.locators.get(key)

    def all_locators(self) -> tuple[RecordLocator, ...]:
        with self._lock:
            return tuple(self._view.locators.values())

    def memory_keys(self, bucket_id: str, *, include_gray: bool = True) -> tuple[str, ...]:
        with self._lock:
            keys = self._view.memory_keys_by_bucket.get(bucket_id, ())
            if include_gray:
                return keys
            return tuple(key for key in keys if not self._view.locators[key].gray)

    def bucket_node_keys(self, bucket_id: str, *, include_gray: bool = True) -> tuple[str, ...]:
        with self._lock:
            keys = self._view.bucket_node_keys_by_bucket.get(bucket_id, ())
            if include_gray:
                return keys
            return tuple(key for key in keys if not self._view.locators[key].gray)

    def bucket_record_keys(self, bucket_id: str, *, include_gray: bool = True) -> tuple[str, ...]:
        return self.memory_keys(bucket_id, include_gray=include_gray) + self.bucket_node_keys(
            bucket_id, include_gray=include_gray
        )

    def get_bucket(self, bucket_id: str) -> BucketInfo | None:
        with self._lock:
            info = self._view.buckets.get(bucket_id)
            return BucketInfo.from_dict(info.to_dict()) if info is not None else None

    def list_buckets(self) -> tuple[BucketInfo, ...]:
        with self._lock:
            return tuple(BucketInfo.from_dict(info.to_dict()) for info in self._view.buckets.values())

    def children(self, parent_bucket_id: str) -> tuple[str, ...]:
        with self._lock:
            return self._view.children_by_parent.get(parent_bucket_id, ())

    def get_child_title_target(self, parent_bucket_id: str, title: str) -> str:
        with self._lock:
            return self._view.title_targets.get((parent_bucket_id, title), "")

    def get_bucket_version(self, bucket_id: str) -> int:
        with self._lock:
            return int(self._view.bucket_versions.get(bucket_id, 0))

    def load_record_node(self, key: str) -> dict[str, Any] | None:
        row = self._connection.execute("SELECT * FROM records WHERE key=?", (key,)).fetchone()
        return self._row_to_state_node(row) if row is not None else None

    def load_negative_weights(self, keys: Iterable[str]) -> dict[str, float]:
        targets = tuple(dict.fromkeys(str(key) for key in keys if str(key)))
        if not targets:
            return {}
        placeholders = ",".join("?" for _ in targets)
        return {
            str(row["key"]): float(row["last_negative_weight"])
            for row in self._connection.execute(
                f"SELECT key, last_negative_weight FROM records WHERE key IN ({placeholders})",
                targets,
            )
        }

    def active_expirations(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (str(row["key"]), str(row["expires_at"]))
            for row in self._connection.execute(
                "SELECT key, expires_at FROM records "
                "WHERE gray=0 AND expires_at IS NOT NULL AND expires_at<>'' "
                "ORDER BY expires_at"
            )
        )

    @staticmethod
    def _row_to_state_node(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "latest_revision": str(row["latest_revision"]),
            "latest_path": str(row["latest_path"]),
            "bucket_id": str(row["bucket_id"]),
            "kind": str(row["kind"]),
            "child_bucket_id": str(row["child_bucket_id"]),
            "confidence_type": str(row["confidence_type"]),
            "gray": bool(row["gray"]),
            "expires_at": row["expires_at"],
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "revision_count": int(row["revision_count"]),
            "latest_evidence_ref": str(row["latest_evidence_ref"]),
            "evidence_history": _json_load(row["evidence_history_json"], []),
            "query_hits": int(row["query_hits"]),
            "last_recalled_at": str(row["last_recalled_at"]),
            "last_compress_penalty_at": str(row["last_compress_penalty_at"]),
            "last_negative_weight": float(row["last_negative_weight"]),
        }

    def load_state_snapshot(self) -> dict[str, Any]:
        keys = {
            str(row["key"]): self._row_to_state_node(row)
            for row in self._connection.execute("SELECT * FROM records ORDER BY rowid")
        }
        row = self._connection.execute(
            "SELECT revision_total FROM repository_meta WHERE singleton=1"
        ).fetchone()
        return {"keys": keys, "revision_total": int(row[0]) if row is not None else 0}

    def load_tree_snapshot(self) -> dict[str, Any]:
        with self._lock:
            buckets = {bucket_id: info.to_dict() for bucket_id, info in self._view.buckets.items()}
            title_maps: dict[str, dict[str, str]] = {}
            for (parent_id, title), child_id in self._view.title_targets.items():
                title_maps.setdefault(parent_id, {})[title] = child_id
            return {
                "root_bucket_id": self._view.root_bucket_id,
                "active_bucket_id": self._view.active_bucket_id,
                "buckets": buckets,
                "child_title_maps": title_maps,
                "updated_at": self._view.topology_updated_at,
            }

    def load_meta(self) -> dict[str, Any]:
        payload = {
            str(row["key"]): _json_load(str(row["value_json"]), None)
            for row in self._connection.execute("SELECT key, value_json FROM engine_meta")
        }
        payload["bucket_versions"] = dict(self._view.bucket_versions)
        payload["auto_split_last_at_by_bucket"] = {
            str(row["bucket_id"]): str(row["last_at"])
            for row in self._connection.execute("SELECT bucket_id, last_at FROM auto_split_state")
        }
        return payload

    def _set_engine_meta_sql(self, key: str, value: Any) -> None:
        self._connection.execute(
            "INSERT INTO engine_meta(key, value_json) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
            (key, _json_dump(value)),
        )

    def set_engine_meta(self, key: str, value: Any) -> None:
        try:
            self._begin()
            self._set_engine_meta_sql(key, value)
            self._connection.execute(
                "UPDATE repository_meta SET updated_at=? WHERE singleton=1",
                (utc_now_iso(),),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc

    def increment_engine_meta(self, key: str, amount: int = 1) -> int:
        try:
            self._begin()
            row = self._connection.execute(
                "SELECT value_json FROM engine_meta WHERE key=?", (key,)
            ).fetchone()
            current = int(_json_load(row[0], 0)) if row is not None else 0
            updated = current + int(amount)
            self._set_engine_meta_sql(key, updated)
            self._commit()
            return updated
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc

    def increment_engine_meta_many(self, deltas: Mapping[str, int]) -> dict[str, int]:
        normalized = {str(key): int(value) for key, value in deltas.items() if int(value) != 0}
        if not normalized:
            return {}
        updated_values: dict[str, int] = {}
        try:
            self._begin()
            for key, amount in normalized.items():
                row = self._connection.execute(
                    "SELECT value_json FROM engine_meta WHERE key=?",
                    (key,),
                ).fetchone()
                current = int(_json_load(row[0], 0)) if row is not None else 0
                updated = current + amount
                self._set_engine_meta_sql(key, updated)
                updated_values[key] = updated
            self._set_engine_meta_sql("updated_at", utc_now_iso())
            self._connection.execute(
                "UPDATE repository_meta SET updated_at=? WHERE singleton=1",
                (utc_now_iso(),),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        return updated_values

    def set_engine_meta_many(self, values: Mapping[str, Any]) -> None:
        if not values:
            return
        try:
            self._begin()
            for key, value in values.items():
                self._set_engine_meta_sql(str(key), value)
            self._connection.execute(
                "UPDATE repository_meta SET updated_at=? WHERE singleton=1",
                (utc_now_iso(),),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc

    def mark_auto_split(self, source_bucket_id: str, successor_bucket_id: str) -> None:
        now = utc_now_iso()
        try:
            self._begin()
            self._connection.execute(
                "INSERT INTO auto_split_state(bucket_id, last_at) VALUES (?, ?) "
                "ON CONFLICT(bucket_id) DO UPDATE SET last_at=excluded.last_at",
                (source_bucket_id, now),
            )
            self._set_engine_meta_sql("last_auto_split_at", now)
            self._set_engine_meta_sql("last_split_source_bucket_id", source_bucket_id)
            self._set_engine_meta_sql("last_split_successor_bucket_id", successor_bucket_id)
            self._set_engine_meta_sql("updated_at", now)
            self._connection.execute(
                "UPDATE repository_meta SET updated_at=? WHERE singleton=1",
                (now,),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc

    def get_last_auto_split_at(self, bucket_id: str) -> str:
        row = self._connection.execute(
            "SELECT last_at FROM auto_split_state WHERE bucket_id=?",
            (bucket_id,),
        ).fetchone()
        return str(row["last_at"]) if row is not None else ""

    def upsert_record(self, locator: RecordLocator, node: Mapping[str, Any]) -> None:
        try:
            self._begin()
            self._upsert_record_sql(locator, node)
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        self._publish_record_deltas((locator,))

    def bulk_upsert_records(
        self,
        records: Iterable[tuple[RecordLocator, Mapping[str, Any]]],
    ) -> int:
        entries = list(records)
        if not entries:
            return 0
        try:
            self._begin()
            self._connection.executemany(
                self._RECORD_UPSERT_SQL,
                (self._record_upsert_parameters(locator, node) for locator, node in entries),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        self._publish_record_deltas(locator for locator, _ in entries)
        return len(entries)

    def commit_record_revision(self, locator: RecordLocator, node: Mapping[str, Any]) -> None:
        """Commit one latest-record pointer and its dirty/version counters atomically."""
        try:
            self._begin()
            self._upsert_record_sql(locator, node)
            self._connection.execute(
                "UPDATE repository_meta SET revision_total=revision_total+1, updated_at=? WHERE singleton=1",
                (utc_now_iso(),),
            )
            context_row = self._connection.execute(
                "SELECT value_json FROM engine_meta WHERE key='context_version'"
            ).fetchone()
            context_version = int(_json_load(context_row[0], 0)) if context_row is not None else 0
            self._set_engine_meta_sql("context_version", context_version + 1)
            self._set_engine_meta_sql("dirty", True)
            self._set_engine_meta_sql("updated_at", utc_now_iso())
            self._connection.execute(
                "INSERT INTO bucket_versions(bucket_id, version) VALUES (?, 1) "
                "ON CONFLICT(bucket_id) DO UPDATE SET version=version+1",
                (locator.bucket_id,),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        self._publish_record_deltas((locator,), increment_bucket_versions=(locator.bucket_id,))

    def update_record_fields(self, key: str, **fields: Any) -> bool:
        allowed = {
            "bucket_id",
            "gray",
            "expires_at",
            "query_hits",
            "last_recalled_at",
            "last_compress_penalty_at",
            "last_negative_weight",
            "updated_at",
            "latest_evidence_ref",
            "evidence_history_json",
        }
        updates = {name: value for name, value in fields.items() if name in allowed}
        if not updates:
            return False
        assignments = ", ".join(f"{name}=?" for name in updates)
        params = list(updates.values()) + [key]
        try:
            self._begin()
            cursor = self._connection.execute(
                f"UPDATE records SET {assignments} WHERE key=?",
                tuple(params),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        changed = cursor.rowcount > 0
        if changed:
            row = self._connection.execute(
                "SELECT key, latest_revision, latest_path, bucket_id, kind, child_bucket_id, "
                "gray, expires_at, updated_at FROM records WHERE key=?",
                (key,),
            ).fetchone()
            if row is not None:
                self._publish_record_deltas((self._locator_from_row(row),))
        return changed

    def delete_records(self, keys: Iterable[str]) -> int:
        targets = tuple(dict.fromkeys(str(key) for key in keys if str(key)))
        if not targets:
            return 0
        placeholders = ",".join("?" for _ in targets)
        try:
            self._begin()
            cursor = self._connection.execute(
                f"DELETE FROM records WHERE key IN ({placeholders})",
                targets,
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        self._publish_record_deltas((), deleted_keys=targets)
        return max(0, int(cursor.rowcount))

    def apply_meta_snapshot(self, meta: Mapping[str, Any]) -> None:
        """Apply compatibility meta changes as row-level deltas, never a whole-file rewrite."""
        versions = meta.get("bucket_versions", {})
        split_state = meta.get("auto_split_last_at_by_bucket", {})
        try:
            self._begin()
            for key, value in meta.items():
                if key not in {"bucket_versions", "auto_split_last_at_by_bucket"}:
                    self._set_engine_meta_sql(str(key), value)
            if isinstance(versions, Mapping):
                for bucket_id, version in versions.items():
                    self._connection.execute(
                        "INSERT INTO bucket_versions(bucket_id, version) VALUES (?, ?) "
                        "ON CONFLICT(bucket_id) DO UPDATE SET version=excluded.version",
                        (str(bucket_id), int(version)),
                    )
            if isinstance(split_state, Mapping):
                for bucket_id, last_at in split_state.items():
                    self._connection.execute(
                        "INSERT INTO auto_split_state(bucket_id, last_at) VALUES (?, ?) "
                        "ON CONFLICT(bucket_id) DO UPDATE SET last_at=excluded.last_at",
                        (str(bucket_id), str(last_at)),
                    )
            self._connection.execute(
                "UPDATE repository_meta SET updated_at=? WHERE singleton=1",
                (utc_now_iso(),),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        self._publish_topology_view()

    def save_tree_snapshot(self, tree: Mapping[str, Any]) -> None:
        """Persist a compatibility topology snapshot in one SQL transaction."""
        raw_buckets = tree.get("buckets", {})
        raw_maps = tree.get("child_title_maps", {})
        if not isinstance(raw_buckets, Mapping) or not isinstance(raw_maps, Mapping):
            raise RepositoryWriteError("invalid topology snapshot")
        buckets = {
            str(bucket_id): BucketInfo.from_dict(dict(raw))
            for bucket_id, raw in raw_buckets.items()
            if isinstance(raw, Mapping)
        }
        root_id = str(tree.get("root_bucket_id", "")).strip()
        active_id = str(tree.get("active_bucket_id", "")).strip() or root_id
        if root_id not in buckets or active_id not in buckets:
            raise RepositoryWriteError("invalid root or active bucket")
        try:
            self._begin()
            for info in buckets.values():
                self._connection.execute(
                    """
                    INSERT INTO buckets(
                        bucket_id, parent_bucket_id, level, title, summary, node_key,
                        summary_status, summary_locked, created_at, updated_at,
                        last_event_at, sealed, sealed_to, archived
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bucket_id) DO UPDATE SET
                        parent_bucket_id=excluded.parent_bucket_id,
                        level=excluded.level,
                        title=excluded.title,
                        summary=excluded.summary,
                        node_key=excluded.node_key,
                        summary_status=excluded.summary_status,
                        summary_locked=excluded.summary_locked,
                        updated_at=excluded.updated_at,
                        last_event_at=excluded.last_event_at,
                        sealed=excluded.sealed,
                        sealed_to=excluded.sealed_to,
                        archived=excluded.archived
                    """,
                    (
                        info.bucket_id,
                        info.parent_bucket_id,
                        int(info.level),
                        info.title,
                        info.summary,
                        info.node_key,
                        info.summary_status,
                        int(info.summary_locked),
                        info.created_at,
                        info.updated_at,
                        float(info.last_event_at),
                        int(info.sealed),
                        info.sealed_to,
                        int(info.archived),
                    ),
                )
            self._connection.execute(
                "INSERT INTO topology_meta(singleton, root_bucket_id, active_bucket_id, updated_at) "
                "VALUES (1, ?, ?, ?) ON CONFLICT(singleton) DO UPDATE SET "
                "root_bucket_id=excluded.root_bucket_id, active_bucket_id=excluded.active_bucket_id, "
                "updated_at=excluded.updated_at",
                (root_id, active_id, utc_now_iso()),
            )
            self._connection.execute("DELETE FROM child_title_maps")
            self._connection.execute("DELETE FROM bucket_edges")
            for info in buckets.values():
                for ordinal, child_id in enumerate(info.children):
                    self._connection.execute(
                        "INSERT INTO bucket_edges(parent_bucket_id, child_bucket_id, ordinal) VALUES (?, ?, ?)",
                        (info.bucket_id, child_id, ordinal),
                    )
            for parent_id, mapping in raw_maps.items():
                if not isinstance(mapping, Mapping):
                    raise RepositoryWriteError(f"invalid title map: {parent_id}")
                for title, child_id in mapping.items():
                    self._connection.execute(
                        "INSERT INTO child_title_maps(parent_bucket_id, title, child_bucket_id) VALUES (?, ?, ?)",
                        (str(parent_id), str(title), str(child_id)),
                    )
            stale = [
                str(row[0])
                for row in self._connection.execute("SELECT bucket_id FROM buckets")
                if str(row[0]) not in buckets
            ]
            for bucket_id in stale:
                self._connection.execute("DELETE FROM buckets WHERE bucket_id=?", (bucket_id,))
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        self._publish_topology_view()

    def replace_topology(
        self,
        *,
        buckets: Iterable[BucketInfo],
        root_bucket_id: str,
        active_bucket_id: str,
        title_maps: Mapping[str, Mapping[str, str]],
    ) -> None:
        bucket_list = list(buckets)
        try:
            self._begin()
            self._connection.execute("DELETE FROM child_title_maps")
            self._connection.execute("DELETE FROM bucket_edges")
            self._connection.execute("DELETE FROM bucket_versions")
            self._connection.execute("DELETE FROM auto_split_state")
            self._connection.execute("DELETE FROM topology_meta")
            self._connection.execute("DELETE FROM buckets")
            for info in bucket_list:
                self._insert_bucket(info)
            for info in bucket_list:
                for ordinal, child_id in enumerate(info.children):
                    self._connection.execute(
                        "INSERT INTO bucket_edges(parent_bucket_id, child_bucket_id, ordinal) VALUES (?, ?, ?)",
                        (info.bucket_id, child_id, ordinal),
                    )
            for parent_id, mapping in title_maps.items():
                for title, child_id in mapping.items():
                    self._connection.execute(
                        "INSERT INTO child_title_maps(parent_bucket_id, title, child_bucket_id) VALUES (?, ?, ?)",
                        (parent_id, title, child_id),
                    )
            self._connection.execute(
                "INSERT INTO topology_meta(singleton, root_bucket_id, active_bucket_id, updated_at) "
                "VALUES (1, ?, ?, ?)",
                (root_bucket_id, active_bucket_id, utc_now_iso()),
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc
        self._publish_topology_view()

    def set_query_cache(
        self,
        cache_key: str,
        result: dict[str, Any],
        *,
        bucket_id: str,
        bucket_version: int,
    ) -> None:
        try:
            self._begin()
            row = self._connection.execute(
                "SELECT COALESCE(MAX(created_seq), 0) + 1 FROM query_cache"
            ).fetchone()
            sequence = int(row[0])
            self._connection.execute(
                """
                INSERT INTO query_cache(
                    cache_key, created_seq, created_at, bucket_id, bucket_version, result_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    created_seq=excluded.created_seq,
                    created_at=excluded.created_at,
                    bucket_id=excluded.bucket_id,
                    bucket_version=excluded.bucket_version,
                    result_json=excluded.result_json
                """,
                (
                    cache_key,
                    sequence,
                    utc_now_iso(),
                    bucket_id,
                    int(bucket_version),
                    _json_dump(result),
                ),
            )
            self._connection.execute(
                "DELETE FROM query_cache WHERE cache_key IN ("
                "SELECT cache_key FROM query_cache ORDER BY created_seq DESC LIMIT -1 OFFSET 5000"
                ")"
            )
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc

    def get_query_cache(self, cache_key: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT result_json FROM query_cache WHERE cache_key=?", (cache_key,)
        ).fetchone()
        if row is None:
            return None
        result = _json_load(str(row[0]), None)
        return result if isinstance(result, dict) else None

    def query_cache_count(self) -> int:
        return int(self._connection.execute("SELECT COUNT(*) FROM query_cache").fetchone()[0])

    def load_query_cache_snapshot(self) -> dict[str, Any]:
        return {
            str(row["cache_key"]): {
                "created_at": str(row["created_at"]),
                "bucket_id": str(row["bucket_id"]),
                "bucket_version": int(row["bucket_version"]),
                "result": _json_load(str(row["result_json"]), {}),
            }
            for row in self._connection.execute(
                "SELECT cache_key, created_at, bucket_id, bucket_version, result_json "
                "FROM query_cache ORDER BY created_seq"
            )
        }

    def clear_query_cache(self) -> None:
        try:
            self._begin()
            self._connection.execute("DELETE FROM query_cache")
            self._commit()
        except Exception as exc:
            self._rollback()
            raise RepositoryWriteError(str(exc)) from exc

    def integrity_check(self) -> dict[str, Any]:
        integrity = str(self._connection.execute("PRAGMA integrity_check").fetchone()[0])
        foreign = [tuple(row) for row in self._connection.execute("PRAGMA foreign_key_check")]
        return {"integrity_check": integrity, "foreign_key_errors": foreign}

    def stats_snapshot(self) -> dict[str, Any]:
        counts = self._connection.execute(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN gray=0 THEN 1 ELSE 0 END) AS active, "
            "SUM(CASE WHEN gray=1 THEN 1 ELSE 0 END) AS gray FROM records"
        ).fetchone()
        repository = self._connection.execute(
            "SELECT revision_total FROM repository_meta WHERE singleton=1"
        ).fetchone()
        meta = self.load_meta()
        meta.update(
            {
                "total_keys": int(counts["total"] or 0),
                "active_keys": int(counts["active"] or 0),
                "gray_keys": int(counts["gray"] or 0),
                "revision_total": int(repository["revision_total"] if repository else 0),
                "event_total": int(meta.get("event_total", 0)),
                "cache_entries": self.query_cache_count(),
                "root_bucket_id": self.root_bucket_id(),
                "active_bucket_id": self.active_bucket_id(),
                "bucket_total": len(self._view.buckets),
            }
        )
        return meta

    def explain_query_plan(self, sql: str, params: tuple[Any, ...] = ()) -> list[str]:
        return [
            str(row["detail"])
            for row in self._connection.execute(f"EXPLAIN QUERY PLAN {sql}", params)
        ]

    def index_diagnostics(self) -> dict[str, int]:
        with self._lock:
            view = self._view
            estimated = self._deep_size(view)
            return {
                "locator_count": len(view.locators),
                "bucket_count": len(view.buckets),
                "estimated_bytes": int(estimated),
            }

    @classmethod
    def _deep_size(cls, value: Any, seen: set[int] | None = None) -> int:
        visited = seen if seen is not None else set()
        object_id = id(value)
        if object_id in visited:
            return 0
        visited.add(object_id)
        size = sys.getsizeof(value)
        if isinstance(value, Mapping):
            return size + sum(
                cls._deep_size(key, visited) + cls._deep_size(item, visited)
                for key, item in value.items()
            )
        if isinstance(value, (tuple, list, set, frozenset)):
            return size + sum(cls._deep_size(item, visited) for item in value)
        if is_dataclass(value) and not isinstance(value, type):
            return size + sum(
                cls._deep_size(getattr(value, field.name), visited)
                for field in fields(value)
            )
        return size

    @staticmethod
    def save_legacy_state(_state: dict[str, Any]) -> None:
        raise RepositoryWriteError("whole-snapshot state writes are forbidden in schema v4")

    @staticmethod
    def save_legacy_tree(_tree: dict[str, Any]) -> None:
        raise RepositoryWriteError("whole-snapshot topology writes are forbidden in schema v4")
