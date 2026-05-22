from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from context_memory.memory import engine as memory_engine
from context_memory.memory.migrations import registry as migration_registry
from context_memory.memory.storage import MemoryStorageV3


class _NoOpStep:
    id = "v1_to_v2_noop"
    from_version = 1
    to_version = 2

    def apply(self, *, storage, context):
        meta = storage.load_meta()
        meta["migration_marker"] = context.run_id
        storage.save_meta(meta)
        return {"applied": True}

    def validate(self, *, storage, context):
        meta = storage.load_meta()
        if str(meta.get("migration_marker", "")) != str(context.run_id):
            raise RuntimeError("missing migration marker")
        return {"validated": True}


class _FailStep:
    id = "v1_to_v2_fail"
    from_version = 1
    to_version = 2

    def apply(self, *, storage, context):
        raise RuntimeError("intentional failure for rollback test")

    def validate(self, *, storage, context):
        return {"validated": False}


@contextmanager
def _temporary_registry():
    old_steps = list(migration_registry._STEPS)
    migration_registry._STEPS.clear()
    try:
        yield
    finally:
        migration_registry._STEPS.clear()
        migration_registry._STEPS.extend(old_steps)
        migration_registry._STEPS.sort(key=lambda s: int(s.from_version))


def _create_engine(base_dir: str | Path):
    return memory_engine.ContextMemoryEngineV3(
        base_dir=base_dir,
        use_mock_llm=True,
        init_config=False,
        auto_resume_pending_jobs=False,
    )


class SchemaMigrationTests(unittest.TestCase):
    def test_missing_schema_file_defaults_to_v1_and_upgrades_to_v2(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                _create_engine(td)
            st = MemoryStorageV3(td)
            info = st.read_schema_version(default_schema_version=1)
            self.assertTrue(st.schema_version_file.exists())
            self.assertEqual(int(info.get("schema_version", -1)), 2)

    def test_data_newer_than_code_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            st = MemoryStorageV3(td)
            st.write_schema_version(schema_version=99, engine_version="future")
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                with self.assertRaises(RuntimeError):
                    _create_engine(td)

    def test_upgrade_success_cleans_checkpoints_and_keeps_single_prebackup(self) -> None:
        with tempfile.TemporaryDirectory() as td, _temporary_registry():
            migration_registry.register_step(_NoOpStep())
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096), patch.object(
                memory_engine,
                "__data_version__",
                2,
            ):
                eng = _create_engine(td)
                status = asyncio.run(eng.migration_status())
            st = MemoryStorageV3(td)
            info = st.read_schema_version(default_schema_version=1)
            self.assertEqual(int(info.get("schema_version", -1)), 2)
            self.assertEqual(bool(status.get("needs_migration", True)), False)
            self.assertTrue(st.pre_upgrade_backup_dir.exists())
            self.assertTrue((st.pre_upgrade_backup_dir / "index" / "state.json").exists())
            run_dirs = list(st.migration_tmp_dir.glob("run_*"))
            self.assertEqual(run_dirs, [])
            journal = st.load_migration_journal()
            self.assertEqual(str(journal.get("status", "")), "success")

    def test_lock_exists_blocks_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as td, _temporary_registry():
            migration_registry.register_step(_NoOpStep())
            st = MemoryStorageV3(td)
            st.migration_lock_file.write_text("held", encoding="utf-8")
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096), patch.object(
                memory_engine,
                "__data_version__",
                2,
            ):
                with self.assertRaises(RuntimeError):
                    _create_engine(td)

    def test_failed_step_keeps_live_dataset_usable_and_marks_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td, _temporary_registry():
            migration_registry.register_step(_FailStep())
            st0 = MemoryStorageV3(td)
            meta0 = st0.load_meta()
            meta0["sentinel"] = "keep_me"
            st0.save_meta(meta0)

            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096), patch.object(
                memory_engine,
                "__data_version__",
                2,
            ):
                with self.assertRaises(RuntimeError):
                    _create_engine(td)

            st = MemoryStorageV3(td)
            meta = st.load_meta()
            self.assertEqual(str(meta.get("sentinel", "")), "keep_me")
            journal = st.load_migration_journal()
            self.assertEqual(str(journal.get("status", "")), "failed")
            run_dirs = list(st.migration_tmp_dir.glob("run_*"))
            self.assertEqual(run_dirs, [])

    def test_runtime_confidence_type_and_last_event_at_propagation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                eng = _create_engine(td)
            root = eng.root_bucket_id()
            child = asyncio.run(
                eng.create_bucket(
                    title="child",
                    parent_bucket_id=root,
                    summary="child summary",
                )
            )
            root_before_info = eng.storage.get_bucket_info(root)
            root_before = float(root_before_info.last_event_at) if root_before_info is not None else 0.0
            add = asyncio.run(eng.add_memory("hello migration v2", bucket_id=child.bucket_id, topic="test"))
            self.assertTrue(bool(add.success))
            rec = eng.storage.get_record(add.key)
            self.assertIsNotNone(rec)
            self.assertEqual(str(rec.confidence_type), "common")
            child_info = eng.storage.get_bucket_info(child.bucket_id)
            root_info = eng.storage.get_bucket_info(root)
            self.assertIsNotNone(child_info)
            self.assertIsNotNone(root_info)
            self.assertGreater(float(child_info.last_event_at), 0.0)
            self.assertGreaterEqual(float(root_info.last_event_at), float(child_info.last_event_at))
            self.assertGreaterEqual(float(root_info.last_event_at), root_before)

    def test_v1_to_v2_migration_patches_old_dataset_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                eng = _create_engine(td)
            root = eng.root_bucket_id()
            child = asyncio.run(
                eng.create_bucket(
                    title="child",
                    parent_bucket_id=root,
                    summary="child summary",
                )
            )
            add = asyncio.run(eng.add_memory("legacy payload", bucket_id=child.bucket_id, topic="legacy"))
            self.assertTrue(bool(add.success))
            record = eng.storage.get_record(add.key)
            self.assertIsNotNone(record)

            latest_path = eng.storage._resolve_root_path(eng.storage.load_state()["keys"][add.key]["latest_path"])
            record_payload = json.loads(latest_path.read_text(encoding="utf-8"))
            record_payload.pop("confidence_type", None)
            latest_path.write_text(json.dumps(record_payload, ensure_ascii=False, indent=2), encoding="utf-8")

            state = eng.storage.load_state()
            node = state.get("keys", {}).get(add.key, {})
            if isinstance(node, dict):
                node.pop("confidence_type", None)
                state["keys"][add.key] = node
                eng.storage.save_state(state)

            bdir = Path(td) / "buckets" / child.bucket_id
            events_path = bdir / "events.ndjson"
            lines = []
            for raw in events_path.read_text(encoding="utf-8").splitlines():
                evt = json.loads(raw)
                if isinstance(evt, dict):
                    evt.pop("confidence_type", None)
                lines.append(json.dumps(evt, ensure_ascii=False, sort_keys=True))
            events_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

            ctx_path = bdir / "context.json"
            ctx = json.loads(ctx_path.read_text(encoding="utf-8"))
            msgs = ctx.get("messages", {})
            if isinstance(msgs, dict):
                for _, msg in msgs.items():
                    if not isinstance(msg, dict):
                        continue
                    data = msg.get("data", {})
                    if not isinstance(data, dict):
                        continue
                    text = data.get("text")
                    if not (isinstance(text, str) and text.startswith("[MEM_EVENT]")):
                        continue
                    evt = json.loads(text[len("[MEM_EVENT]") :])
                    if isinstance(evt, dict):
                        evt.pop("confidence_type", None)
                        data["text"] = "[MEM_EVENT]" + json.dumps(
                            evt,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
            ctx_path.write_text(json.dumps(ctx, ensure_ascii=False, indent=2), encoding="utf-8")

            tree = eng.storage.load_bucket_tree()
            buckets = tree.get("buckets", {})
            if isinstance(buckets, dict):
                for bid, raw in buckets.items():
                    if isinstance(raw, dict):
                        raw.pop("last_event_at", None)
                        buckets[bid] = raw
                tree["buckets"] = buckets
                eng.storage.save_bucket_tree(tree)

            eng.storage.write_schema_version(schema_version=1, engine_version="legacy")

            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                _create_engine(td)

            st = MemoryStorageV3(td)
            rec_after = st.get_record(add.key)
            self.assertIsNotNone(rec_after)
            self.assertEqual(str(rec_after.confidence_type), "common")
            tree_after = st.load_bucket_tree()
            for _, raw in tree_after.get("buckets", {}).items():
                if isinstance(raw, dict):
                    self.assertIn("last_event_at", raw)
            ctx_after = json.loads(ctx_path.read_text(encoding="utf-8"))
            for _, msg in ctx_after.get("messages", {}).items():
                if not isinstance(msg, dict):
                    continue
                data = msg.get("data", {})
                text = data.get("text") if isinstance(data, dict) else None
                if isinstance(text, str) and text.startswith("[MEM_EVENT]"):
                    evt = json.loads(text[len("[MEM_EVENT]") :])
                    if isinstance(evt, dict):
                        self.assertTrue(str(evt.get("confidence_type", "")).strip())
            for raw in events_path.read_text(encoding="utf-8").splitlines():
                evt = json.loads(raw)
                if isinstance(evt, dict):
                    self.assertTrue(str(evt.get("confidence_type", "")).strip())


if __name__ == "__main__":
    unittest.main()
