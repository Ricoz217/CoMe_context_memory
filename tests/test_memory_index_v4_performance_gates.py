from __future__ import annotations

import ast
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

from context_memory.memory import engine as memory_engine
from context_memory.memory.index_repository import IndexRepository, RecordLocator
from context_memory.memory.models import MemoryRecord, normalize_relations


def _create_engine(base_dir: Path):
    with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
        return memory_engine.ContextMemoryEngineV3(
            base_dir=base_dir,
            use_mock_llm=True,
            init_config=False,
            auto_manage=False,
            auto_resume_pending_jobs=False,
        )


def _record(engine, bucket_id: str, title: str) -> MemoryRecord:
    return MemoryRecord(
        key=engine.storage.generate_key(),
        revision_id=engine.storage.generate_revision_id(),
        kind="memory",
        bucket_id=bucket_id,
        title=title,
        summary=title,
        content=f"content:{title}",
        weight=0.5,
        event="ADD",
        gray=False,
        relations=normalize_relations({}),
        confidence_type="common",
    )


def _node(locator: RecordLocator) -> dict[str, object]:
    return {
        "latest_revision": locator.latest_revision,
        "latest_path": locator.latest_path,
        "bucket_id": locator.bucket_id,
        "kind": locator.kind,
        "child_bucket_id": locator.child_bucket_id,
        "confidence_type": "common",
        "gray": locator.gray,
        "expires_at": locator.expires_at,
        "created_at": locator.updated_at,
        "updated_at": locator.updated_at,
        "revision_count": 1,
        "latest_evidence_ref": "",
        "evidence_history": [],
        "query_hits": 0,
        "last_recalled_at": "",
        "last_compress_penalty_at": "",
        "last_negative_weight": 0.0,
    }


def test_non_empty_optimize_applies_successor_rebuild(tmp_path: Path) -> None:
    asyncio.run(_test_non_empty_optimize_applies_successor_rebuild(tmp_path))


async def _test_non_empty_optimize_applies_successor_rebuild(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    try:
        old_root = engine.root_bucket_id()
        record = _record(engine, old_root, "optimize-me")
        await engine._runtime.run_storage_task(engine.storage.write_memory_record, record)
        llm_plan = {
            "skip_optimize": False,
            "parent_flat_keys": ["memory_1"],
            "groups": [],
            "parent_summary": "optimized root",
            "parent_content": "optimized root content",
            "metadata_update": {},
        }
        repository = engine.storage.repository
        assert repository is not None
        with (
            patch.object(engine.pipeline, "optimize", new=AsyncMock(return_value=llm_plan)),
            patch.object(
                repository,
                "_build_view",
                side_effect=AssertionError("runtime writes must not rebuild the full locator view"),
            ),
        ):
            result = await engine.optimize(bucket_id=old_root, reason="v4_regression")

        assert result.success is True
        assert result.moved_items == 1
        assert result.bucket_id != old_root
        assert engine.root_bucket_id() == result.bucket_id
        old_info = engine.storage.get_bucket_info(old_root)
        assert old_info is not None and old_info.sealed and old_info.sealed_to == result.bucket_id
    finally:
        await engine.close(wait=True)


def test_global_recall_loads_all_scanned_buckets_in_one_batch(tmp_path: Path) -> None:
    asyncio.run(_test_global_recall_loads_all_scanned_buckets_in_one_batch(tmp_path))


async def _test_global_recall_loads_all_scanned_buckets_in_one_batch(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    try:
        root = engine.root_bucket_id()
        child = await engine.create_bucket(root, title="child", summary="child")
        await engine._runtime.run_storage_task(engine.storage.write_memory_record, _record(engine, root, "root-memory"))
        await engine._runtime.run_storage_task(
            engine.storage.write_memory_record,
            _record(engine, child.bucket_id, "child-memory"),
        )
        service = engine._query
        original_batch = engine.storage.load_buckets_snapshot
        with (
            patch.object(engine.storage, "load_buckets_snapshot", wraps=original_batch) as batch_loader,
            patch.object(
                engine.storage,
                "load_bucket_snapshot",
                side_effect=AssertionError("global recall must not perform N+1 bucket loads"),
            ),
        ):
            record_boost, bucket_boost = await service._build_global_recall_boosts(
                root_bucket_id=root,
                query_text="memory",
                include_gray=False,
                mode="semantic",
                top_n=10,
                top_m=5,
                depth_limit=3,
                time_budget_ms=1000,
            )

        assert batch_loader.call_count == 1
        assert record_boost
        assert root in bucket_boost or child.bucket_id in bucket_boost
    finally:
        await engine.close(wait=True)


def test_resident_index_100k_stays_below_128_mib_and_single_delta_is_o1(tmp_path: Path) -> None:
    repository = IndexRepository.create_empty(tmp_path / "index" / "memory_index.sqlite3")
    try:
        root = repository.root_bucket_id()
        updated_at = "2026-01-01T00:00:00+00:00"

        def entries():
            for index in range(100_000):
                key = f"mem_20260101000000_{index:032x}"
                revision = f"rev_20260101000000_{index:032x}"
                locator = RecordLocator(
                    key=key,
                    latest_revision=revision,
                    latest_path=f"memories/{key}/{revision}.json",
                    bucket_id=root,
                    kind="memory",
                    child_bucket_id="",
                    gray=False,
                    expires_at=None,
                    updated_at=updated_at,
                )
                yield locator, _node(locator)

        assert repository.bulk_upsert_records(entries()) == 100_000
        diagnostics = repository.index_diagnostics()
        assert diagnostics["locator_count"] == 100_000
        assert diagnostics["estimated_bytes"] <= 128 * 1024 * 1024

        next_key = "mem_20260101000000_ffffffffffffffffffffffffffffffff"
        next_revision = "rev_20260101000000_ffffffffffffffffffffffffffffffff"
        next_locator = RecordLocator(
            key=next_key,
            latest_revision=next_revision,
            latest_path=f"memories/{next_key}/{next_revision}.json",
            bucket_id=root,
            kind="memory",
            child_bucket_id="",
            gray=False,
            expires_at=None,
            updated_at=updated_at,
        )
        with patch.object(
            repository,
            "_build_view",
            side_effect=AssertionError("single record delta rebuilt the full view"),
        ):
            repository.upsert_record(next_locator, _node(next_locator))
        assert repository.get_locator(next_key) == next_locator
    finally:
        repository.close()


def test_static_business_storage_gates() -> None:
    memory_root = Path(memory_engine.__file__).resolve().parent
    business_files = [memory_root / "engine.py", *sorted((memory_root / "services").glob("*.py"))]
    combined = "\n".join(path.read_text(encoding="utf-8-sig") for path in business_files)
    assert "list_bucket_records(" not in combined
    assert "list_latest_records(" not in combined
    for method in (
        "load_state",
        "load_bucket_tree",
        "load_meta",
        "load_cache",
        "save_state",
        "save_bucket_tree",
        "save_meta",
        "save_cache",
    ):
        assert f".storage.{method}(" not in combined

    sqlite_imports = []
    for path in memory_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        if any(
            (isinstance(node, ast.Import) and any(alias.name == "sqlite3" for alias in node.names))
            or (isinstance(node, ast.ImportFrom) and node.module == "sqlite3")
            for node in ast.walk(tree)
        ):
            sqlite_imports.append(path.name)
    assert sqlite_imports == ["index_repository.py"]

    assert set(RecordLocator.__dataclass_fields__) == {
        "key",
        "latest_revision",
        "latest_path",
        "bucket_id",
        "kind",
        "child_bucket_id",
        "gray",
        "expires_at",
        "updated_at",
    }


def test_warm_locator_and_topology_reads_do_not_touch_sqlite(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    try:
        repository = engine.storage.repository
        assert repository is not None
        root = engine.root_bucket_id()
        connection = repository._connection

        class _NoExecuteConnection:
            def execute(self, *_args, **_kwargs):
                raise AssertionError("warm resident read touched SQLite")

            def __getattr__(self, name: str):
                return getattr(connection, name)

        repository._connection = _NoExecuteConnection()  # type: ignore[assignment]
        try:
            assert repository.root_bucket_id() == root
            assert repository.active_bucket_id() == root
            assert repository.get_bucket(root) is not None
            assert repository.get_bucket_version(root) == 0
            assert engine.storage.get_key_node("missing") is None
        finally:
            repository._connection = connection
    finally:
        engine.shutdown(wait=True)
