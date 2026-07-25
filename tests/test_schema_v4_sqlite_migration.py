from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from context_memory.memory.index_repository import IndexRepository
from context_memory.memory.migrations.steps.v3_to_v4 import _V3ToV4Step
from context_memory.memory.migrations.types import MigrationContext
from context_memory.memory.models import MemoryRecord
from context_memory.memory.storage import MemoryStorageV3


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _legacy_v3_storage(root: Path) -> tuple[MemoryStorageV3, str, str]:
    storage = MemoryStorageV3(root)
    bucket_id = storage.get_root_bucket_id()
    record = MemoryRecord(
        key=storage.generate_key(),
        revision_id=storage.generate_revision_id(),
        kind="memory",
        bucket_id=bucket_id,
        title="title",
        summary="summary",
        content="content stays in revision json",
        weight=0.7,
        event="ADD",
        gray=False,
    )
    storage.write_memory_record(record)
    storage.write_schema_version(schema_version=3, engine_version="0.4.0")
    return storage, bucket_id, record.key


def test_v3_to_v4_imports_index_and_removes_only_legacy_index_files(tmp_path: Path) -> None:
    storage, bucket_id, key = _legacy_v3_storage(tmp_path)
    alias_path = storage.buckets_dir / bucket_id / "alias_map.json"
    context_path = storage.buckets_dir / bucket_id / "context.json"
    revision_path = next((storage.memories_dir / key).glob("*.json"))
    preserved = {
        alias_path: _hash(alias_path),
        context_path: _hash(context_path),
        revision_path: _hash(revision_path),
    }
    context = MigrationContext(
        run_id="test",
        from_version=3,
        to_version=4,
        workspace_root=tmp_path,
    )

    step = _V3ToV4Step()
    result = step.apply(storage=storage, context=context)
    validated = step.validate(storage=storage, context=context)

    assert result["records_imported"] == 1
    assert validated["integrity_check"] == "ok"
    assert storage.sqlite_index_file.exists()
    for path in (storage.state_file, storage.bucket_tree_file, storage.meta_file, storage.cache_file):
        assert not path.exists()
    for path, expected_hash in preserved.items():
        assert path.exists()
        assert _hash(path) == expected_hash

    repo = IndexRepository(storage.sqlite_index_file)
    assert repo.get_locator(key) is not None
    assert repo.get_locator(key).bucket_id == bucket_id
    assert repo.query_cache_count() == 0
    repo.close()


def test_v3_to_v4_rejects_missing_latest_revision_without_deleting_legacy(tmp_path: Path) -> None:
    storage, _bucket_id, key = _legacy_v3_storage(tmp_path)
    revision_path = next((storage.memories_dir / key).glob("*.json"))
    revision_path.unlink()
    context = MigrationContext(
        run_id="test",
        from_version=3,
        to_version=4,
        workspace_root=tmp_path,
    )

    with pytest.raises(RuntimeError, match="revision file not found"):
        _V3ToV4Step().apply(storage=storage, context=context)

    assert storage.state_file.exists()
    assert storage.bucket_tree_file.exists()
    assert storage.meta_file.exists()
    assert storage.cache_file.exists()
    assert not storage.sqlite_index_file.exists()
