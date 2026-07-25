from __future__ import annotations

import asyncio
import json
import re
import tempfile
import unittest
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from context_memory.memory import engine as memory_engine
from context_memory.memory.aliasing import AliasPayloadError, AliasStore, AliasTable


REAL_ID_RE = re.compile(r"(?:mem|bucket|rev)_[0-9]{14}_[0-9a-f]{32}")


class _CallbackStorage:
    """Minimal legacy storage used to exercise AliasTable's callback fallback."""

    def __init__(self) -> None:
        self.maps: dict[str, dict[str, str]] = {}
        self.frozen: set[str] = set()

    def _map(self, bucket_id: str) -> dict[str, str]:
        return self.maps.setdefault(bucket_id, {})

    def get_or_create_alias(self, bucket_id: str, real_key: str, key_type: str) -> str:
        amap = self._map(bucket_id)
        typed_key = f"{key_type}:{real_key}"
        if typed_key not in amap:
            sequence = 1 + sum(key.startswith(f"{key_type}:") for key in amap)
            alias = f"{key_type}_{sequence}"
            amap[typed_key] = alias
            amap[alias] = real_key
        return amap[typed_key]

    def find_alias(self, bucket_id: str, real_key: str, key_type: str) -> str | None:
        return self._map(bucket_id).get(f"{key_type}:{real_key}")

    def resolve_alias(self, bucket_id: str, alias: str, expected_type: str | None = None) -> str:
        if expected_type and not alias.startswith(f"{expected_type}_"):
            raise TypeError(alias)
        value = self._map(bucket_id).get(alias)
        if value is None:
            raise KeyError(alias)
        return value

    def alias_map_version(self, bucket_id: str) -> int:
        return 1

    def freeze_alias_map(self, bucket_id: str) -> None:
        self.frozen.add(bucket_id)


def _create_engine(base_dir: str | Path):
    return memory_engine.ContextMemoryEngineV3(
        base_dir=base_dir,
        use_mock_llm=True,
        init_config=False,
        auto_resume_pending_jobs=False,
    )

@contextmanager
def _temporary_engine(prefix: str):
    with tempfile.TemporaryDirectory(prefix=prefix) as td:
        with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
            engine = _create_engine(td)
        try:
            yield engine
        finally:
            engine.shutdown(wait=True)


class AliasTableTests(unittest.TestCase):
    def test_callback_storage_fallback_uses_the_same_generic_converter(self) -> None:
        storage = _CallbackStorage()
        table = AliasTable(storage, "legacy_bucket")
        mem_a = "mem_20260715000000_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        mem_b = "mem_20260715000000_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

        self.assertEqual(table.to_alias(mem_a), "memory_1")
        self.assertEqual(table.to_alias(mem_a, allow_create=False), "memory_1")
        with self.assertRaises(AliasPayloadError):
            table.to_alias(mem_b, allow_create=False)

        encoded = table.encode_tree(
            {mem_a: [f"prefix:{mem_a}", (mem_b, 7)]},
        )
        self.assertEqual(
            encoded,
            {"memory_1": ["prefix:memory_1", ("memory_2", 7)]},
        )
        with self.assertRaises(AliasPayloadError):
            table.encode_tree({mem_a: "first", "memory_1": "second"})

        table.freeze()
        self.assertEqual(storage.frozen, {"legacy_bucket"})

    def test_to_real_many_returns_successes_and_preserves_strict_errors(self) -> None:
        storage = _CallbackStorage()
        table = AliasTable(storage, "legacy_bucket")
        mem_id = "mem_20260715000000_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        bucket_id = "bucket_20260715000000_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        memory_alias = table.to_alias(mem_id)
        bucket_alias = table.to_alias(bucket_id)

        self.assertEqual(
            table.to_real_many([memory_alias, "memory_999", bucket_alias, memory_alias]),
            {memory_alias: mem_id, bucket_alias: bucket_id},
        )
        self.assertEqual(
            table.to_real_many([memory_alias, bucket_alias], expected_type="memory"),
            {memory_alias: mem_id},
        )
        with self.assertRaises(KeyError):
            table.to_real_many(["memory_999"], strict=True)
        with self.assertRaises(TypeError):
            table.to_real_many([bucket_alias], expected_type="memory", strict=True)
        with self.assertRaises(TypeError):
            table.to_real_many(memory_alias)

    def test_scalar_mapping_is_append_only_and_bucket_scoped(self) -> None:
        with _temporary_engine("cm_alias_table_") as eng:
            root = eng.root_bucket_id()
            child = asyncio.run(eng.create_bucket(root, title="child", summary="child"))
            store = AliasStore(eng.storage)
            root_table = store.open(root)
            child_table = store.open(child.bucket_id)
            mem_a = eng.storage.generate_key()
            mem_b = eng.storage.generate_key()

            self.assertEqual(root_table.to_alias(mem_a), "memory_1")
            self.assertEqual(root_table.to_alias(mem_a), "memory_1")
            self.assertEqual(root_table.to_alias(mem_b), "memory_2")
            self.assertEqual(child_table.to_alias(mem_a), "memory_1")
            self.assertEqual(root_table.to_real("memory_1"), mem_a)
            with self.assertRaises(KeyError):
                root_table.to_real("memory_999")

    def test_encode_tree_converts_keys_lists_and_embedded_ids(self) -> None:
        with _temporary_engine("cm_alias_tree_") as eng:
            root = eng.root_bucket_id()
            table = AliasStore(eng.storage).open(root)
            mem_id = eng.storage.generate_key()
            rev_id = eng.storage.generate_revision_id()
            payload = {
                root: {
                    "content": {
                        mem_id: {
                            "label": f"{root} chunk 1/1",
                            "source": [f"{root}_memories", mem_id, rev_id],
                        }
                    }
                }
            }

            encoded = table.encode_tree(payload)
            encoded_text = json.dumps(encoded, ensure_ascii=False)

            self.assertIsNone(REAL_ID_RE.search(encoded_text))
            self.assertIn("bucket_1", encoded)
            self.assertIn("memory_1", encoded["bucket_1"]["content"])
            self.assertIn("bucket_1_memories", encoded_text)
            self.assertIn("revision_1", encoded_text)
            table.assert_safe(encoded)

    def test_decode_tree_restores_structured_aliases_without_rewriting_prose(self) -> None:
        with _temporary_engine("cm_alias_decode_") as eng:
            root = eng.root_bucket_id()
            table = AliasStore(eng.storage).open(root)
            mem_id = eng.storage.generate_key()
            rev_id = eng.storage.generate_revision_id()
            encoded = table.encode_tree(
                {
                    mem_id: {
                        "key": mem_id,
                        "revision_id": rev_id,
                        "items": [mem_id, {"bucket": root}],
                    }
                }
            )
            encoded["memory_1"]["comment"] = "memory_1 is a display label"

            decoded = table.decode_tree(encoded)

            self.assertIn(mem_id, decoded)
            self.assertEqual(decoded[mem_id]["key"], mem_id)
            self.assertEqual(decoded[mem_id]["revision_id"], rev_id)
            self.assertEqual(decoded[mem_id]["items"], [mem_id, {"bucket": root}])
            self.assertEqual(decoded[mem_id]["comment"], "memory_1 is a display label")

    def test_decode_tree_rejects_unknown_aliases_and_key_collisions(self) -> None:
        with _temporary_engine("cm_alias_decode_strict_") as eng:
            table = AliasStore(eng.storage).open(eng.root_bucket_id())
            mem_id = eng.storage.generate_key()
            self.assertEqual(table.to_alias(mem_id), "memory_1")

            with self.assertRaises(AliasPayloadError):
                table.decode_tree({"key": "memory_999"})
            with self.assertRaises(AliasPayloadError):
                table.decode_tree({"memory_1": "first", mem_id: "second"})

            self.assertEqual(
                table.decode_tree({"key": "memory_999"}, strict_unknown=False),
                {"key": "memory_999"},
            )

    def test_assert_safe_detects_real_ids_in_any_key_or_string(self) -> None:
        with _temporary_engine("cm_alias_safe_") as eng:
            table = AliasStore(eng.storage).open(eng.root_bucket_id())
            mem_id = eng.storage.generate_key()

            with self.assertRaises(AliasPayloadError):
                table.assert_safe({mem_id: {}})
            with self.assertRaises(AliasPayloadError):
                table.assert_safe({"label": f"prefix:{mem_id}:suffix"})

    def test_encode_tree_rejects_alias_key_collisions(self) -> None:
        with _temporary_engine("cm_alias_collision_") as eng:
            table = AliasStore(eng.storage).open(eng.root_bucket_id())
            mem_id = eng.storage.generate_key()

            with self.assertRaises(AliasPayloadError):
                table.encode_tree({mem_id: "first", "memory_1": "second"})

    def test_failed_commit_does_not_publish_mapping_to_cache(self) -> None:
        with _temporary_engine("cm_alias_commit_") as eng:
            root = eng.root_bucket_id()
            table = AliasStore(eng.storage).open(root)
            mem_id = eng.storage.generate_key()

            with patch.object(eng.storage, "commit_alias_map", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    table.to_alias(mem_id)

            self.assertIsNone(eng.storage.find_alias(root, mem_id, "memory"))

    def test_sealed_table_rejects_new_aliases(self) -> None:
        with _temporary_engine("cm_alias_sealed_") as eng:
            root = eng.root_bucket_id()
            table = AliasStore(eng.storage).open(root)
            table.to_alias(eng.storage.generate_key())
            snapshot_hash = table.snapshot_hash()
            table.freeze()
            sealed_hash = table.snapshot_hash()

            with self.assertRaises(RuntimeError):
                table.to_alias(eng.storage.generate_key())
            self.assertEqual(len(snapshot_hash), 64)
            self.assertNotEqual(sealed_hash, snapshot_hash)
            self.assertEqual(table.snapshot_hash(), sealed_hash)

    def test_validation_read_only_and_async_store_paths(self) -> None:
        with _temporary_engine("cm_alias_validation_") as eng:
            root = eng.root_bucket_id()
            store = AliasStore(eng.storage)
            table = store.open(root)
            mem_id = eng.storage.generate_key()

            with self.assertRaises(ValueError):
                store.open("")
            with self.assertRaises(ValueError):
                table.to_alias("not-a-real-id")
            with self.assertRaises(ValueError):
                table.to_alias(mem_id, key_type="bucket")
            with self.assertRaises(ValueError):
                table.to_real("")
            with self.assertRaises(AliasPayloadError):
                table.to_alias(mem_id, allow_create=False)

            alias = table.to_alias(mem_id)
            self.assertEqual(table.to_alias(mem_id, allow_create=False), alias)
            self.assertEqual(table.encode_text(f"prefix:{mem_id}:suffix"), f"prefix:{alias}:suffix")
            self.assertEqual(table.encode_tree((mem_id,)), (alias,))
            self.assertGreater(table.map_version(), 1)
            self.assertEqual(asyncio.run(store.prepare(root, {mem_id: mem_id})), {alias: alias})
            self.assertEqual(asyncio.run(store.restore(root, {alias: alias})), {mem_id: mem_id})
            stale_version = table.map_version()
            table.to_alias(eng.storage.generate_key())
            with self.assertRaises(AliasPayloadError):
                table.encode_tree({"safe": True}, map_version=stale_version)
            with self.assertRaises(AliasPayloadError):
                table.decode_tree({"safe": True}, map_version=stale_version)

    def test_engine_async_paths_use_alias_table_instead_of_compatibility_codec(self) -> None:
        with _temporary_engine("cm_alias_engine_core_") as eng:
            root = eng.root_bucket_id()
            mem_id = eng.storage.generate_key()

            with patch.object(
                eng.alias_codec,
                "build_llm_view",
                side_effect=AssertionError("compatibility codec used"),
            ):
                encoded = asyncio.run(eng.prepare_alias_payload(root, {"key": mem_id}))
            with patch.object(
                eng.alias_codec,
                "resolve_llm_output",
                side_effect=AssertionError("compatibility codec used"),
            ):
                decoded = asyncio.run(eng.restore_alias_payload(root, encoded))

            self.assertEqual(decoded, {"key": mem_id})

    def test_corrupt_or_counter_inconsistent_maps_fail_closed(self) -> None:
        with self.assertRaises(ValueError):
            AliasTable(object(), "")

        with _temporary_engine("cm_alias_corrupt_") as eng:
            root = eng.root_bucket_id()
            table = AliasStore(eng.storage).open(root)
            amap = eng.storage.load_alias_map(root)
            amap["real_to_alias"] = {"memory:broken": "memory_1"}

            with self.assertRaises(AliasPayloadError):
                table.encode_tree({"value": "safe"})

        with _temporary_engine("cm_alias_structure_") as eng:
            root = eng.root_bucket_id()
            table = AliasStore(eng.storage).open(root)
            eng.storage.load_alias_map(root)["alias_to_real"] = []

            with self.assertRaises(AliasPayloadError):
                table.encode_tree({"value": "safe"})

        with _temporary_engine("cm_alias_counter_") as eng:
            root = eng.root_bucket_id()
            table = AliasStore(eng.storage).open(root)
            first = eng.storage.generate_key()
            second = eng.storage.generate_key()
            self.assertEqual(table.to_alias(first), "memory_1")
            eng.storage.load_alias_map(root)["counters"]["memory"] = 0

            with self.assertRaises(AliasPayloadError):
                table.to_alias(second)

    def test_concurrent_allocation_has_no_duplicates_or_lost_entries(self) -> None:
        with _temporary_engine("cm_alias_concurrency_") as eng:
            root = eng.root_bucket_id()
            table = AliasStore(eng.storage).open(root)
            real_ids = [eng.storage.generate_key() for _ in range(40)]

            with ThreadPoolExecutor(max_workers=8) as pool:
                aliases = list(pool.map(table.to_alias, real_ids + real_ids))

            first_pass = aliases[: len(real_ids)]
            second_pass = aliases[len(real_ids) :]
            self.assertEqual(len(set(first_pass)), len(real_ids))
            self.assertEqual(first_pass, second_pass)
            reloaded = AliasStore(eng.storage).open(root)
            for real_id, alias in zip(real_ids, first_pass):
                self.assertEqual(reloaded.to_real(alias), real_id)

    def test_async_prepare_does_not_block_event_loop(self) -> None:
        with _temporary_engine("cm_alias_async_") as eng:
            table = AliasStore(eng.storage).open(eng.root_bucket_id())
            mem_id = eng.storage.generate_key()

            async def _run() -> int:
                ticks = 0

                async def _heartbeat() -> None:
                    nonlocal ticks
                    for _ in range(5):
                        await asyncio.sleep(0.01)
                        ticks += 1

                original = table._encode_tree_transaction

                def _slow_encode(value, *, allow_create=True, map_version=None):
                    import time

                    time.sleep(0.08)
                    return original(value, allow_create=allow_create, map_version=map_version)

                with patch.object(table, "_encode_tree_transaction", side_effect=_slow_encode):
                    await asyncio.gather(table.prepare({"key": mem_id}), _heartbeat())
                return ticks

            self.assertEqual(asyncio.run(_run()), 5)


if __name__ == "__main__":
    unittest.main()
