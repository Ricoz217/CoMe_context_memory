from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from context_memory.LLM_connect import Prompts, TextPrompt, ToolInput
from context_memory.memory import engine as memory_engine
from context_memory.memory.models import BUCKET_KIND_MEMORY, MemoryRecord, normalize_relations


def _create_engine(base_dir: str | Path):
    return memory_engine.ContextMemoryEngineV3(
        base_dir=base_dir,
        use_mock_llm=True,
        init_config=False,
        auto_resume_pending_jobs=False,
    )


class AdvanceQueryTests(unittest.TestCase):
    def test_restructure_memory_is_stable_and_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                eng = _create_engine(td)

            root = eng.root_bucket_id()
            child_a = asyncio.run(eng.create_bucket(root, title="child_a", summary="a"))
            child_b = asyncio.run(eng.create_bucket(root, title="child_b", summary="b"))
            info_a = eng.storage.get_bucket_info(child_a.bucket_id)
            info_b = eng.storage.get_bucket_info(child_b.bucket_id)
            assert info_a is not None and info_b is not None
            info_a.last_event_at = 20.0
            info_b.last_event_at = 10.0
            eng.storage.update_bucket_info(info_a)
            eng.storage.update_bucket_info(info_b)

            rec_z = MemoryRecord(
                key="mem_99999999999999_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                revision_id=eng.storage.generate_revision_id(),
                kind=BUCKET_KIND_MEMORY,
                bucket_id=root,
                title="z",
                summary="z",
                content="content-z",
                weight=0.5,
                event="ADD",
                gray=False,
                relations=normalize_relations({}),
                confidence_type="common",
            )
            rec_a = MemoryRecord(
                key="mem_11111111111111_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                revision_id=eng.storage.generate_revision_id(),
                kind=BUCKET_KIND_MEMORY,
                bucket_id=root,
                title="a",
                summary="a",
                content="content-a",
                weight=0.5,
                event="ADD",
                gray=False,
                relations=normalize_relations({}),
                confidence_type="common",
            )
            eng.storage.write_memory_record(rec_z)
            eng.storage.write_memory_record(rec_a)

            svc = eng._advance_query_service
            node1 = svc._advance_collect_bucket_tree(
                bucket_id=root,
                include_gray=False,
                max_expand_depth=None,
                depth=0,
                visited=set(),
            )
            node2 = svc._advance_collect_bucket_tree(
                bucket_id=root,
                include_gray=False,
                max_expand_depth=None,
                depth=0,
                visited=set(),
            )
            payload1 = svc._advance_render_top_payload(node1)
            payload2 = svc._advance_render_top_payload(node2)
            self.assertEqual(payload1, payload2)

            root_content = payload1[root]["content"]
            ordered_keys = list(root_content.keys())
            self.assertEqual(
                ordered_keys,
                [
                    "mem_11111111111111_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "mem_99999999999999_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    child_b.bucket_id,
                    child_a.bucket_id,
                ],
            )

    def test_single_shot_overflow_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                eng = _create_engine(td)
            threshold = int(eng.max_context_window * 0.8)
            with patch.object(eng._advance_query_service, "_advance_payload_tokens", return_value=threshold + 1):
                with self.assertRaises(RuntimeError):
                    asyncio.run(
                        eng.advance_query(
                            command="test",
                            mode="single_shot",
                            enable_aliasing=False,
                        )
                    )

    def test_chunk_tools_disabled_and_final_tools_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                eng = _create_engine(td)
            root = eng.root_bucket_id()
            info = eng.storage.get_bucket_info(root)
            assert info is not None

            calls: list[bool] = []

            async def _fake_llm_request(*, allow_tools: bool, **kwargs):
                calls.append(bool(allow_tools))
                return Prompts(TextPrompt("assistant", "ok"))

            chunk_specs = [
                {"label": f"{root} chunk 1/1", "source": [f"{root}_memories"], "content": {"k1": {"content": "v1"}}}
            ]
            with patch.object(eng._advance_query_service, "_advance_llm_request", side_effect=_fake_llm_request):
                chunk_items = asyncio.run(
                    eng._advance_query_service._advance_execute_chunks(
                        chunk_specs=chunk_specs,
                        bucket_id=root,
                        bucket_metadata=eng._advance_query_service._advance_bucket_metadata(info),
                        command="cmd",
                        system_text="sys",
                        llm_preset=None,
                        alias_bucket_id=root,
                        enable_aliasing=False,
                        parallel_limit=4,
                        audit=False,
                    )
                )
            self.assertFalse(any(calls))

            calls.clear()
            tool = ToolInput(lambda: "ok", "noop")
            with (
                patch.object(eng._advance_query_service, "_advance_llm_request", side_effect=_fake_llm_request),
                patch.object(eng._advance_query_service, "_advance_payload_tokens", return_value=100),
            ):
                asyncio.run(
                    eng._advance_query_service._advance_reduce_result_items(
                        bucket_id=root,
                        bucket_metadata=eng._advance_query_service._advance_bucket_metadata(info),
                        items=chunk_items,
                        command="cmd",
                        system_text="sys",
                        llm_preset=None,
                        threshold_tokens=500,
                        alias_bucket_id=root,
                        enable_aliasing=False,
                        parallel_limit=4,
                        audit=False,
                        allow_tools_on_final=True,
                        final_tool_input=tool,
                    )
                )
            self.assertEqual(calls, [True])

    def test_aliasing_only_grows_target_bucket_map(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                eng = _create_engine(td)
            root = eng.root_bucket_id()
            child = asyncio.run(eng.create_bucket(root, title="child_x", summary="x"))

            before_root = eng.storage.load_alias_map(root)
            before_child = eng.storage.load_alias_map(child.bucket_id)
            root_before_count = len(before_root.get("real_to_alias", {}))
            child_before_count = len(before_child.get("real_to_alias", {}))

            async def _fake_llm_request(**kwargs):
                return Prompts(TextPrompt("assistant", "ok"))

            with (
                patch.object(eng._advance_query_service, "_advance_llm_request", side_effect=_fake_llm_request),
                patch.object(eng._advance_query_service, "_advance_count_tokens_exact", return_value=1),
            ):
                asyncio.run(
                    eng.advance_query(
                        command="hello",
                        mode="single_shot",
                        enable_aliasing=True,
                    )
                )

            after_root = eng.storage.load_alias_map(root)
            after_child = eng.storage.load_alias_map(child.bucket_id)
            self.assertGreaterEqual(len(after_root.get("real_to_alias", {})), root_before_count)
            self.assertEqual(len(after_child.get("real_to_alias", {})), child_before_count)


if __name__ == "__main__":
    unittest.main()
