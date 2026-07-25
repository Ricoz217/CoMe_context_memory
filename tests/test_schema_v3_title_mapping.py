from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from context_memory.memory import engine as memory_engine
from context_memory.memory.storage import MemoryStorageV3


def _create_engine(base_dir: Path):
    with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
        return memory_engine.ContextMemoryEngineV3(
            base_dir=base_dir,
            use_mock_llm=True,
            init_config=False,
            auto_manage=False,
            auto_resume_pending_jobs=False,
        )


def _build_v2_store(base_dir: Path) -> tuple[MemoryStorageV3, str, str]:
    storage = MemoryStorageV3(base_dir)
    root = storage.get_root_bucket_id()
    parent = storage.create_bucket(
        parent_bucket_id=root,
        level=2,
        title="parent",
        summary="parent",
        node_key=storage.generate_key(),
    )
    child = storage.create_bucket(
        parent_bucket_id=parent.bucket_id,
        level=3,
        title="child",
        summary="child",
        node_key=storage.generate_key(),
    )
    (base_dir / "bucket_mapping.json").write_text(
        json.dumps({"parent": parent.bucket_id, "child": child.bucket_id}, ensure_ascii=False),
        encoding="utf-8",
    )
    storage.write_schema_version(schema_version=2, engine_version="legacy-v2")
    return storage, parent.bucket_id, child.bucket_id


def test_v2_global_mapping_migrates_to_parent_scoped_tree(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    old_storage, parent_id, child_id = _build_v2_store(base_dir)
    root_id = old_storage.get_root_bucket_id()

    engine = _create_engine(base_dir)
    tree = engine.storage.load_bucket_tree()

    assert tree["child_title_maps"][root_id]["parent"] == parent_id
    assert tree["child_title_maps"][parent_id]["child"] == child_id
    assert not (base_dir / "bucket_mapping.json").exists()
    assert (engine.storage.pre_upgrade_backup_dir / "bucket_mapping.json").exists()
    assert engine.storage.read_schema_version(default_schema_version=1)["schema_version"] == 4
    assert not engine.storage.state_file.exists()
    assert not engine.storage.bucket_tree_file.exists()
    assert not engine.storage.meta_file.exists()
    assert not engine.storage.cache_file.exists()
    engine.shutdown(wait=True)


def test_v3_migration_preserves_valid_alias_map_bytes(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    storage, _, child_id = _build_v2_store(base_dir)
    alias_path = storage.buckets_dir / child_id / "alias_map.json"
    payload = storage.load_alias_map(child_id)
    real_id = storage.generate_key()
    payload["real_to_alias"] = {f"memory:{real_id}": "memory_1"}
    payload["alias_to_real"] = {"memory_1": {"key_type": "memory", "real_key": real_id}}
    payload["counters"]["memory"] = 1
    storage.commit_alias_map(child_id, payload)
    before = hashlib.sha256(alias_path.read_bytes()).hexdigest()

    engine = _create_engine(base_dir)
    after = hashlib.sha256((engine.storage.buckets_dir / child_id / "alias_map.json").read_bytes()).hexdigest()

    assert after == before


def test_v3_migration_normalizes_alias_metadata_without_renumbering(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    storage, _, child_id = _build_v2_store(base_dir)
    alias_path = storage.buckets_dir / child_id / "alias_map.json"
    real_id = storage.generate_key()
    alias_path.write_text(
        json.dumps(
            {
                "real_to_alias": {f"memory:{real_id}": "memory_7"},
                "alias_to_real": {"memory_7": {"key_type": "memory", "real_key": real_id}},
                "counters": {"memory": 1},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    engine = _create_engine(base_dir)
    migrated = json.loads((engine.storage.buckets_dir / child_id / "alias_map.json").read_text(encoding="utf-8"))

    assert migrated["real_to_alias"][f"memory:{real_id}"] == "memory_7"
    assert migrated["alias_to_real"]["memory_7"]["real_key"] == real_id
    assert migrated["counters"] == {"memory": 7, "bucket": 0, "revision": 0, "ref": 0}
    assert migrated["map_version"] >= 1
    assert migrated["sealed"] is False
    assert migrated["bucket_id"] == child_id


def test_corrupt_alias_map_aborts_v3_migration_and_preserves_live_v2(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    storage, _, child_id = _build_v2_store(base_dir)
    alias_path = storage.buckets_dir / child_id / "alias_map.json"
    alias_path.write_text(
        json.dumps(
            {
                "bucket_id": child_id,
                "map_version": 1,
                "sealed": False,
                "real_to_alias": {"memory:broken": "memory_1"},
                "alias_to_real": {},
                "counters": {"memory": 1, "bucket": 0, "revision": 0, "ref": 0},
            }
        ),
        encoding="utf-8",
    )
    before = alias_path.read_bytes()

    with pytest.raises(RuntimeError):
        _create_engine(base_dir)

    after_storage = MemoryStorageV3(base_dir)
    assert after_storage.read_schema_version(default_schema_version=1)["schema_version"] == 2
    assert alias_path.read_bytes() == before
    assert after_storage.load_migration_journal()["status"] == "failed"


def test_v3_migration_drops_stale_legacy_target(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    storage, parent_id, child_id = _build_v2_store(base_dir)
    mapping_path = base_dir / "bucket_mapping.json"
    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    mapping["stale"] = "bucket_20000101000000_00000000000000000000000000000000"
    mapping_path.write_text(json.dumps(mapping), encoding="utf-8")

    engine = _create_engine(base_dir)
    tree = engine.storage.load_bucket_tree()

    assert tree["child_title_maps"][parent_id]["child"] == child_id
    assert all("stale" not in parent_map for parent_map in tree["child_title_maps"].values())


def test_v3_migration_keeps_terminal_archived_alias_map_but_drops_legacy_route(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    storage, _, child_id = _build_v2_store(base_dir)
    child = storage.get_bucket_info(child_id)
    assert child is not None
    child.sealed = True
    child.archived = True
    child.sealed_to = ""
    storage.update_bucket_info(child)
    storage.freeze_alias_map(child_id)
    before_alias = (storage.buckets_dir / child_id / "alias_map.json").read_bytes()

    engine = _create_engine(base_dir)
    tree = engine.storage.load_bucket_tree()

    assert all(child_id not in parent_map.values() for parent_map in tree["child_title_maps"].values())
    assert (engine.storage.buckets_dir / child_id / "alias_map.json").read_bytes() == before_alias


def test_v3_migration_rejects_existing_target_with_broken_topology(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    storage, parent_id, child_id = _build_v2_store(base_dir)
    tree = storage.load_bucket_tree()
    tree["buckets"][parent_id]["children"].remove(child_id)
    storage.save_bucket_tree(tree)
    before = storage.bucket_tree_file.read_bytes()

    with pytest.raises(RuntimeError):
        _create_engine(base_dir)

    after_storage = MemoryStorageV3(base_dir)
    assert after_storage.read_schema_version(default_schema_version=1)["schema_version"] == 2
    assert after_storage.bucket_tree_file.read_bytes() == before
    assert after_storage.load_migration_journal()["status"] == "failed"


def test_v3_migration_rejects_wrong_existing_alias_bucket_id(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    storage, _, child_id = _build_v2_store(base_dir)
    alias_path = storage.buckets_dir / child_id / "alias_map.json"
    payload = storage.load_alias_map(child_id)
    payload["bucket_id"] = storage.get_root_bucket_id()
    alias_path.write_text(json.dumps(payload), encoding="utf-8")
    before = alias_path.read_bytes()

    with pytest.raises(RuntimeError):
        _create_engine(base_dir)

    assert alias_path.read_bytes() == before
    assert MemoryStorageV3(base_dir).read_schema_version(default_schema_version=1)["schema_version"] == 2


def test_v3_migration_dry_run_reports_step_without_mutating_data(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    storage, _, _ = _build_v2_store(base_dir)
    before_tree = storage.bucket_tree_file.read_bytes()
    before_mapping = (base_dir / "bucket_mapping.json").read_bytes()
    with patch.object(memory_engine, "__data_version__", 2):
        engine = _create_engine(base_dir)

    with patch.object(memory_engine, "__data_version__", 3):
        result = asyncio.run(engine.migrate_schema(dry_run=True))

    assert result["dry_run"] is True
    assert [step["id"] for step in result["plan"]] == ["v2_to_v3_parent_scoped_title_maps"]
    assert storage.bucket_tree_file.read_bytes() == before_tree
    assert (base_dir / "bucket_mapping.json").read_bytes() == before_mapping
    assert storage.read_schema_version(default_schema_version=1)["schema_version"] == 2
