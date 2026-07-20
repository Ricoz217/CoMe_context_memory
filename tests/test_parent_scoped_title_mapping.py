from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from context_memory.memory import engine as memory_engine


def _create_engine(base_dir: Path):
    with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
        return memory_engine.ContextMemoryEngineV3(
            base_dir=base_dir,
            use_mock_llm=True,
            init_config=False,
            auto_manage=False,
            auto_resume_pending_jobs=False,
        )


def test_set_bucket_is_scoped_to_its_parent(tmp_path: Path) -> None:
    asyncio.run(_test_set_bucket_is_scoped_to_its_parent(tmp_path))


async def _test_set_bucket_is_scoped_to_its_parent(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    parent_a = await engine.set_bucket("parent-a")
    parent_b = await engine.set_bucket("parent-b")

    child_a = await parent_a.set_bucket("shared-title")
    child_b = await parent_b.set_bucket("shared-title")

    assert child_a.bucket_id != child_b.bucket_id
    assert engine.storage.get_bucket_info(child_a.bucket_id).parent_bucket_id == parent_a.bucket_id
    assert engine.storage.get_bucket_info(child_b.bucket_id).parent_bucket_id == parent_b.bucket_id


def test_deleted_parent_title_does_not_resurrect_old_children(tmp_path: Path) -> None:
    asyncio.run(_test_deleted_parent_title_does_not_resurrect_old_children(tmp_path))


async def _test_deleted_parent_title_does_not_resurrect_old_children(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    old_parent = await engine.set_bucket("group-parent")
    old_child = await old_parent.set_bucket("member")

    deleted = await old_parent.delete_memory(old_parent.bucket_id, reason="reset")
    assert deleted.success is True

    new_parent = await engine.set_bucket("group-parent")
    new_child = await new_parent.set_bucket("member")

    assert new_parent.bucket_id != old_parent.bucket_id
    assert new_child.bucket_id != old_child.bucket_id
    assert engine.storage.get_bucket_info(new_child.bucket_id).parent_bucket_id == new_parent.bucket_id


def test_same_parent_set_bucket_is_concurrent_setdefault(tmp_path: Path) -> None:
    asyncio.run(_test_same_parent_set_bucket_is_concurrent_setdefault(tmp_path))


async def _test_same_parent_set_bucket_is_concurrent_setdefault(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    parent = await engine.set_bucket("parent")

    handles = await asyncio.gather(*(parent.set_bucket("same") for _ in range(20)))

    assert len({handle.bucket_id for handle in handles}) == 1


def test_explicit_create_bucket_does_not_register_setdefault_title(tmp_path: Path) -> None:
    asyncio.run(_test_explicit_create_bucket_does_not_register_setdefault_title(tmp_path))


async def _test_explicit_create_bucket_does_not_register_setdefault_title(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    parent = await engine.set_bucket("parent")
    explicit_a = await parent.create_bucket(title="duplicate")
    explicit_b = await parent.create_bucket(title="duplicate")

    setdefault_handle = await parent.set_bucket("duplicate")

    assert explicit_a.bucket_id != explicit_b.bucket_id
    assert setdefault_handle.bucket_id not in {explicit_a.bucket_id, explicit_b.bucket_id}


def test_parent_scoped_mapping_survives_restart(tmp_path: Path) -> None:
    asyncio.run(_test_parent_scoped_mapping_survives_restart(tmp_path))


async def _test_parent_scoped_mapping_survives_restart(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    first = _create_engine(base_dir)
    parent = await first.set_bucket("parent")
    child = await parent.set_bucket("child")

    second = _create_engine(base_dir)
    parent_again = await second.set_bucket("parent")
    child_again = await parent_again.set_bucket("child")

    assert parent_again.bucket_id == parent.bucket_id
    assert child_again.bucket_id == child.bucket_id


def test_child_successor_replaces_parent_title_target(tmp_path: Path) -> None:
    asyncio.run(_test_child_successor_replaces_parent_title_target(tmp_path))


async def _test_child_successor_replaces_parent_title_target(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    parent = await engine.set_bucket("parent")
    source = await parent.set_bucket("child")
    successor = await parent.create_bucket(title="child-successor")

    engine._seal_bucket_unlocked(
        source_bucket_id=source.bucket_id,
        successor_bucket_id=successor.bucket_id,
    )

    routed = await parent.set_bucket("child")
    assert routed.bucket_id == successor.bucket_id


def test_parent_successor_inherits_child_title_map(tmp_path: Path) -> None:
    asyncio.run(_test_parent_successor_inherits_child_title_map(tmp_path))


async def _test_parent_successor_inherits_child_title_map(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    source = await engine.set_bucket("parent")
    child = await source.set_bucket("member")
    successor = await engine.create_bucket(
        title="parent-successor",
        parent_bucket_id=engine.root_bucket_id(),
    )
    engine.storage.reparent_bucket(
        bucket_id=child.bucket_id,
        new_parent_bucket_id=successor.bucket_id,
        preserve_old_title_map=True,
    )

    engine._seal_bucket_unlocked(
        source_bucket_id=source.bucket_id,
        successor_bucket_id=successor.bucket_id,
    )

    routed = await engine.set_bucket_with_id("member", successor.bucket_id)
    assert routed.bucket_id == child.bucket_id
    tree = engine.storage.load_bucket_tree()
    assert tree["child_title_maps"][source.bucket_id]["member"] == child.bucket_id
    assert tree["child_title_maps"][successor.bucket_id]["member"] == child.bucket_id

    second = await engine.create_bucket(
        title="parent-successor-2",
        parent_bucket_id=engine.root_bucket_id(),
    )
    engine.storage.reparent_bucket(
        bucket_id=child.bucket_id,
        new_parent_bucket_id=second.bucket_id,
        preserve_old_title_map=True,
    )
    engine._seal_bucket_unlocked(
        source_bucket_id=successor.bucket_id,
        successor_bucket_id=second.bucket_id,
    )
    assert engine.storage.validate_dataset_layout()["success"] is True
    assert (await engine.set_bucket_with_id("member", source.bucket_id)).bucket_id == child.bucket_id


def test_root_successor_inherits_child_title_map(tmp_path: Path) -> None:
    asyncio.run(_test_root_successor_inherits_child_title_map(tmp_path))


async def _test_root_successor_inherits_child_title_map(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    root_id = engine.root_bucket_id()
    child = await engine.set_bucket("member")
    root = engine.storage.get_bucket_info(root_id)
    assert root is not None
    successor = engine.storage.create_bucket(
        parent_bucket_id=None,
        level=root.level,
        title="ROOT-successor",
        summary=root.summary,
        node_key=engine.storage.generate_key(),
    )
    engine.storage.reparent_bucket(
        bucket_id=child.bucket_id,
        new_parent_bucket_id=successor.bucket_id,
        preserve_old_title_map=True,
    )

    engine._seal_bucket_unlocked(source_bucket_id=root_id, successor_bucket_id=successor.bucket_id)

    routed = await engine.set_bucket("member")
    assert routed.bucket_id == child.bucket_id


def test_reparent_title_conflict_is_atomic(tmp_path: Path) -> None:
    asyncio.run(_test_reparent_title_conflict_is_atomic(tmp_path))


async def _test_reparent_title_conflict_is_atomic(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    parent_a = await engine.set_bucket("parent-a")
    parent_b = await engine.set_bucket("parent-b")
    child_a = await parent_a.set_bucket("shared")
    await parent_b.set_bucket("shared")
    before = copy.deepcopy(engine.storage.load_bucket_tree())

    try:
        engine.storage.reparent_bucket(bucket_id=child_a.bucket_id, new_parent_bucket_id=parent_b.bucket_id)
    except ValueError:
        pass
    else:
        raise AssertionError("reparent conflict must fail")

    assert engine.storage.load_bucket_tree() == before


def test_gc_removes_deleted_bucket_title_map_and_inbound_refs(tmp_path: Path) -> None:
    asyncio.run(_test_gc_removes_deleted_bucket_title_map_and_inbound_refs(tmp_path))


async def _test_gc_removes_deleted_bucket_title_map_and_inbound_refs(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    root_id = engine.root_bucket_id()
    source = engine.storage.create_bucket(
        parent_bucket_id=root_id,
        level=2,
        title="source",
        summary="",
        node_key=engine.storage.generate_key(),
        mapping_title="source",
    )
    child = engine.storage.create_bucket(
        parent_bucket_id=source.bucket_id,
        level=3,
        title="child",
        summary="",
        node_key=engine.storage.generate_key(),
        mapping_title="child",
    )
    successor = engine.storage.create_bucket(
        parent_bucket_id=root_id,
        level=2,
        title="successor",
        summary="",
        node_key=engine.storage.generate_key(),
    )
    engine.storage.reparent_bucket(
        bucket_id=child.bucket_id,
        new_parent_bucket_id=successor.bucket_id,
        preserve_old_title_map=True,
    )
    engine._seal_bucket_unlocked(
        source_bucket_id=source.bucket_id,
        successor_bucket_id=successor.bucket_id,
    )
    tree = engine.storage.load_bucket_tree()
    tree["buckets"][source.bucket_id]["updated_at"] = "2000-01-01T00:00:00+00:00"
    engine.storage.save_bucket_tree(tree)
    engine._gc_archived_bucket_retention_days = 1

    result = await engine.gc_storage(dry_run=False, reason="title-map-test")

    assert result.success is True
    tree = engine.storage.load_bucket_tree()
    assert source.bucket_id not in tree["buckets"]
    assert source.bucket_id not in tree["child_title_maps"]
    assert all(
        source.bucket_id not in parent_map.values()
        for parent_map in tree["child_title_maps"].values()
    )


def test_group_main_dialog_reset_memory_does_not_restore_old_member_persona(tmp_path: Path) -> None:
    asyncio.run(_test_group_main_dialog_reset_memory_does_not_restore_old_member_persona(tmp_path))


async def _test_group_main_dialog_reset_memory_does_not_restore_old_member_persona(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    group_id = "10001"
    user_id = "20002"
    persona_root = await engine.set_bucket("[PERSONA]")
    old_parent = await persona_root.set_bucket(f"[PERSONA]_[PARENT][GROUP]{group_id}")
    old_group = await old_parent.set_bucket(f"[PERSONA]_[GROUP]{group_id}")
    old_member = await old_parent.set_bucket(f"[PERSONA]_[GROUP]{group_id}_[MEMBER]{user_id}")
    old_memory = await old_member.add_memory("old member persona sentinel")
    assert old_memory.success is True

    deleted = await old_parent.delete_memory(old_parent.bucket_id, reason="user reset memory")
    assert deleted.success is True

    new_parent = await persona_root.set_bucket(f"[PERSONA]_[PARENT][GROUP]{group_id}")
    new_group = await new_parent.set_bucket(f"[PERSONA]_[GROUP]{group_id}")
    new_member = await new_parent.set_bucket(f"[PERSONA]_[GROUP]{group_id}_[MEMBER]{user_id}")
    memories = await new_member.list_memories()

    assert new_parent.bucket_id != old_parent.bucket_id
    assert new_group.bucket_id != old_group.bucket_id
    assert new_member.bucket_id != old_member.bucket_id
    assert old_memory.key not in json.dumps(memories.to_dict(), ensure_ascii=False)


def test_add_memory_from_dir_scopes_same_folder_name_to_each_branch(tmp_path: Path) -> None:
    asyncio.run(_test_add_memory_from_dir_scopes_same_folder_name_to_each_branch(tmp_path))


async def _test_add_memory_from_dir_scopes_same_folder_name_to_each_branch(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    import_root = tmp_path / "import"
    (import_root / "branch-a" / "shared").mkdir(parents=True)
    (import_root / "branch-b" / "shared").mkdir(parents=True)
    (import_root / "branch-a" / "shared" / "a.txt").write_text("alpha", encoding="utf-8")
    (import_root / "branch-b" / "shared" / "b.txt").write_text("beta", encoding="utf-8")

    result = await engine.add_memory_from_dir(
        str(import_root),
        auto_create_sub_buckets=True,
        force_split=False,
    )

    branch_a = await engine.set_bucket("branch-a")
    branch_b = await engine.set_bucket("branch-b")
    shared_a = await branch_a.set_bucket("shared")
    shared_b = await branch_b.set_bucket("shared")
    assert result["success"] is True
    assert shared_a.bucket_id != shared_b.bucket_id
    assert engine.storage.get_bucket_info(shared_a.bucket_id).parent_bucket_id == branch_a.bucket_id
    assert engine.storage.get_bucket_info(shared_b.bucket_id).parent_bucket_id == branch_b.bucket_id


def test_corrupt_child_title_maps_fails_closed(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    tree = engine.storage.load_bucket_tree()
    tree["child_title_maps"] = []
    engine.storage.save_bucket_tree(tree)

    with pytest.raises(ValueError, match="child_title_maps"):
        engine.storage.get_child_title_target(engine.root_bucket_id(), "anything")

