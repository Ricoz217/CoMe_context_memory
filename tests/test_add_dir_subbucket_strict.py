from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from context_memory import ContextMemoryConfig, ContextMemoryEngineV3


@pytest.mark.asyncio
async def test_add_dir_auto_create_sub_buckets_creates_folder_buckets_only(tmp_path: Path) -> None:
    base_dir = tmp_path / "store"
    input_dir = tmp_path / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)

    root_file = input_dir / "root_file.txt"
    mod_a_file = input_dir / "module_a" / "a.txt"
    deep_file = input_dir / "module_a" / "deep" / "deep.txt"
    mod_b_file = input_dir / "module_b" / "b.txt"

    mod_a_file.parent.mkdir(parents=True, exist_ok=True)
    deep_file.parent.mkdir(parents=True, exist_ok=True)
    mod_b_file.parent.mkdir(parents=True, exist_ok=True)

    root_file.write_text("root level record", encoding="utf-8")
    mod_a_file.write_text("module a record", encoding="utf-8")
    deep_file.write_text("deep module record", encoding="utf-8")
    mod_b_file.write_text("module b record", encoding="utf-8")

    cfg = ContextMemoryConfig(
        base_dir=base_dir,
        llm_preset="CONTEXT_MEMORY",
        image_llm_preset="KIMI2.6",
        use_mock_llm=True,
        init_config=False,
        auto_manage=False,
    )
    engine = ContextMemoryEngineV3(config=cfg)
    root = await engine.set_bucket("ADD_DIR_TREE_STRICT")

    result = await root.add_memory_from_dir(
        str(input_dir),
        auto_create_sub_buckets=True,
        force_split=False,
        dedup_in_bucket=False,
    )
    assert result.get("success") is True
    assert int(result.get("processed_files", 0)) == 4
    assert int(result.get("success_count", 0)) == 4

    per_file_added_keys = result.get("per_file_added_keys", {})
    assert isinstance(per_file_added_keys, dict)
    assert str(root_file) in per_file_added_keys
    assert str(mod_a_file) in per_file_added_keys
    assert str(deep_file) in per_file_added_keys
    assert str(mod_b_file) in per_file_added_keys

    buckets = engine.list_buckets()
    parent_title_to_id = {(str(b.parent_bucket_id), str(b.title)): str(b.bucket_id) for b in buckets}

    root_bucket_id = str(root.bucket_id)
    mod_a_bucket_id = parent_title_to_id.get((root_bucket_id, "module_a"), "")
    mod_b_bucket_id = parent_title_to_id.get((root_bucket_id, "module_b"), "")
    deep_bucket_id = parent_title_to_id.get((mod_a_bucket_id, "deep"), "")

    assert mod_a_bucket_id
    assert mod_b_bucket_id
    assert deep_bucket_id

    bucket_titles = {str(b.title) for b in buckets}
    assert "root_file.txt" not in bucket_titles
    assert "a.txt" not in bucket_titles
    assert "deep.txt" not in bucket_titles
    assert "b.txt" not in bucket_titles

    def _first_key(file_path: Path) -> str:
        keys = per_file_added_keys.get(str(file_path), [])
        assert isinstance(keys, list) and keys
        return str(keys[0])

    root_mem = await engine.get_memory(_first_key(root_file))
    mod_a_mem = await engine.get_memory(_first_key(mod_a_file))
    deep_mem = await engine.get_memory(_first_key(deep_file))
    mod_b_mem = await engine.get_memory(_first_key(mod_b_file))

    assert root_mem is not None and str(root_mem.bucket_id) == root_bucket_id
    assert mod_a_mem is not None and str(mod_a_mem.bucket_id) == mod_a_bucket_id
    assert deep_mem is not None and str(deep_mem.bucket_id) == deep_bucket_id
    assert mod_b_mem is not None and str(mod_b_mem.bucket_id) == mod_b_bucket_id

