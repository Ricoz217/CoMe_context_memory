from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from context_memory.memory.index_repository import (
    IndexRepository,
    RecordLocator,
    RepositoryWriteError,
)
from context_memory.memory.models import BucketInfo


def _bucket(bucket_id: str, *, parent: str | None = None, children: list[str] | None = None) -> BucketInfo:
    return BucketInfo(
        bucket_id=bucket_id,
        parent_bucket_id=parent,
        level=1 if parent is None else 2,
        title=bucket_id,
        summary=f"summary:{bucket_id}",
        node_key="",
        children=list(children or []),
    )


def _node(key: str, bucket_id: str, *, kind: str = "memory", child: str = "") -> dict:
    return {
        "latest_revision": f"rev_{key}",
        "latest_path": f"memories/{key}/rev_{key}.json",
        "bucket_id": bucket_id,
        "kind": kind,
        "child_bucket_id": child,
        "confidence_type": "common",
        "gray": False,
        "expires_at": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "revision_count": 1,
        "latest_evidence_ref": "",
        "evidence_history": [],
        "query_hits": 0,
        "last_recalled_at": "",
        "last_compress_penalty_at": "",
        "last_negative_weight": 0.0,
    }


def test_repository_imports_legacy_index_and_rebuilds_memory_view(tmp_path: Path) -> None:
    root = _bucket("bucket_root", children=["bucket_child"])
    child = _bucket("bucket_child", parent=root.bucket_id)
    state = {
        "keys": {
            "mem_1": _node("mem_1", root.bucket_id),
            "mem_2": _node("mem_2", child.bucket_id),
            "node_1": _node("node_1", root.bucket_id, kind="bucket", child=child.bucket_id),
        },
        "revision_total": 3,
    }
    tree = {
        "root_bucket_id": root.bucket_id,
        "active_bucket_id": child.bucket_id,
        "buckets": {root.bucket_id: root.to_dict(), child.bucket_id: child.to_dict()},
        "child_title_maps": {root.bucket_id: {"child": child.bucket_id}},
    }
    meta = {
        "dirty": True,
        "context_version": 7,
        "prompt_version": "v3",
        "bucket_versions": {root.bucket_id: 4, child.bucket_id: 2},
        "auto_split_last_at_by_bucket": {child.bucket_id: "2026-01-01T00:00:00+00:00"},
    }

    repo = IndexRepository.create_from_legacy(
        tmp_path / "index" / "memory_index.sqlite3",
        state=state,
        tree=tree,
        meta=meta,
    )

    assert repo.root_bucket_id() == root.bucket_id
    assert repo.active_bucket_id() == child.bucket_id
    assert repo.get_bucket_version(root.bucket_id) == 4
    assert repo.get_child_title_target(root.bucket_id, "child") == child.bucket_id
    assert repo.memory_keys(root.bucket_id) == ("mem_1",)
    assert repo.bucket_node_keys(root.bucket_id) == ("node_1",)
    assert repo.memory_keys(child.bucket_id) == ("mem_2",)
    assert repo.get_locator("mem_1") == RecordLocator(
        key="mem_1",
        latest_revision="rev_mem_1",
        latest_path="memories/mem_1/rev_mem_1.json",
        bucket_id=root.bucket_id,
        kind="memory",
        child_bucket_id="",
        gray=False,
        expires_at=None,
        updated_at="2026-01-01T00:00:00+00:00",
    )

    diagnostics = repo.index_diagnostics()
    assert diagnostics["locator_count"] == 3
    assert diagnostics["bucket_count"] == 2
    assert diagnostics["estimated_bytes"] > 0
    assert "content" not in RecordLocator.__dataclass_fields__
    assert "relations" not in RecordLocator.__dataclass_fields__

    repo.close()
    reopened = IndexRepository(tmp_path / "index" / "memory_index.sqlite3")
    assert reopened.get_locator("mem_2") is not None
    assert reopened.load_meta()["context_version"] == 7
    assert reopened.integrity_check() == {"integrity_check": "ok", "foreign_key_errors": []}
    reopened.close()


def test_repository_delta_is_published_only_after_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = IndexRepository.create_empty(tmp_path / "index" / "memory_index.sqlite3")
    root_id = repo.root_bucket_id()
    locator = RecordLocator(
        key="mem_1",
        latest_revision="rev_1",
        latest_path="memories/mem_1/rev_1.json",
        bucket_id=root_id,
        kind="memory",
        child_bucket_id="",
        gray=False,
        expires_at=None,
        updated_at="2026-01-01T00:00:00+00:00",
    )

    def fail_commit() -> None:
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(repo, "_commit", fail_commit)
    with pytest.raises(RepositoryWriteError, match="disk full"):
        repo.upsert_record(locator, _node("mem_1", root_id))

    assert repo.get_locator("mem_1") is None
    assert repo.memory_keys(root_id) == ()
    repo.close()


def test_query_cache_keeps_latest_5000_entries(tmp_path: Path) -> None:
    repo = IndexRepository.create_empty(tmp_path / "index" / "memory_index.sqlite3")
    root_id = repo.root_bucket_id()
    for index in range(5005):
        repo.set_query_cache(
            f"cache_{index}",
            {"index": index},
            bucket_id=root_id,
            bucket_version=0,
        )

    assert repo.get_query_cache("cache_0") is None
    assert repo.get_query_cache("cache_4") is None
    assert repo.get_query_cache("cache_5") == {"index": 5}
    assert repo.get_query_cache("cache_5004") == {"index": 5004}
    assert repo.query_cache_count() == 5000
    repo.close()


def test_whole_legacy_snapshot_save_is_rejected_in_v4(tmp_path: Path) -> None:
    repo = IndexRepository.create_empty(tmp_path / "index" / "memory_index.sqlite3")
    with pytest.raises(RepositoryWriteError, match="whole-snapshot"):
        repo.save_legacy_state({"keys": {}})
    with pytest.raises(RepositoryWriteError, match="whole-snapshot"):
        repo.save_legacy_tree({"buckets": {}})
    repo.close()
