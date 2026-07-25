from __future__ import annotations

import asyncio
import re
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from context_memory.LLM_connect import Prompts, TextPrompt
from context_memory.memory import engine as memory_engine
from context_memory.memory.aliasing import AliasPayloadError
from context_memory.memory.models import BUCKET_KIND_MEMORY, MemoryRecord, normalize_relations


REAL_ID_RE = re.compile(r"(?:mem|bucket|rev)_[0-9]{14}_[0-9a-f]{32}")


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


def _write_memory(eng, bucket_id: str, title: str) -> str:
    key = eng.storage.generate_key()
    eng.storage.write_memory_record(
        MemoryRecord(
            key=key,
            revision_id=eng.storage.generate_revision_id(),
            kind=BUCKET_KIND_MEMORY,
            bucket_id=bucket_id,
            title=title,
            summary=title,
            content="secret-content",
            weight=0.5,
            event="ADD",
            gray=False,
            relations=normalize_relations({}),
            confidence_type="common",
        )
    )
    return key


class AdvanceQueryAliasSafetyTests(unittest.TestCase):
    def test_single_shot_and_best_effort_send_alias_only_markdown(self) -> None:
        with _temporary_engine("cm_advance_alias_") as eng:
            root = eng.root_bucket_id()
            child = asyncio.run(eng.create_bucket(root, title="child", summary="child"))
            mem_id = _write_memory(eng, root, "root-memory")
            _write_memory(eng, child.bucket_id, "child-memory")
            captured: list[tuple[str, str]] = []

            async def _fake_request(*, system_text: str, user_markdown: str, **_kwargs):
                captured.append((system_text, user_markdown))
                return Prompts(TextPrompt("assistant", "ok"))

            async def _run() -> None:
                svc = eng._advance
                with (
                    patch.object(svc, "_advance_llm_request", side_effect=_fake_request),
                    patch.object(svc, "_advance_count_tokens_exact", return_value=1),
                ):
                    await eng.advance_query(
                        command=f"inspect {mem_id}",
                        system_prompt=f"system for {root}",
                        mode="single_shot",
                        enable_aliasing=True,
                    )
                    await eng.advance_query(
                        command=f"inspect {mem_id}",
                        system_prompt=f"system for {root}",
                        mode="best_effort_full_view",
                        enable_aliasing=True,
                    )

            asyncio.run(_run())
            self.assertEqual(len(captured), 2)
            for system_text, user_markdown in captured:
                self.assertIsNone(REAL_ID_RE.search(system_text))
                self.assertIsNone(REAL_ID_RE.search(user_markdown))
                self.assertIn("memory_", user_markdown)
                self.assertIn("bucket_", user_markdown)

    def test_chunk_request_converts_real_ids_in_keys_labels_and_sources(self) -> None:
        with _temporary_engine("cm_advance_chunk_alias_") as eng:
            root = eng.root_bucket_id()
            mem_id = _write_memory(eng, root, "root-memory")
            info = eng.storage.get_bucket_info(root)
            assert info is not None
            captured: list[str] = []

            async def _fake_request(*, user_markdown: str, **_kwargs):
                captured.append(user_markdown)
                return Prompts(TextPrompt("assistant", "ok"))

            async def _run() -> None:
                svc = eng._advance
                eng._alias.begin_session()
                try:
                    with patch.object(svc, "_advance_llm_request", side_effect=_fake_request):
                        await svc._advance_execute_chunks(
                            chunk_specs=[
                                {
                                    "label": f"{root} chunk 1/1",
                                    "source": [f"{root}_memories"],
                                    "content": {mem_id: {"content": "secret-content"}},
                                }
                            ],
                            bucket_id=root,
                            bucket_metadata=svc._advance_bucket_metadata(info),
                            command=f"inspect {mem_id}",
                            system_text=f"system for {root}",
                            llm_preset=None,
                            alias_bucket_id=root,
                            enable_aliasing=True,
                            parallel_limit=1,
                            audit=False,
                        )
                finally:
                    eng._alias.end_session()

            asyncio.run(_run())
            self.assertEqual(len(captured), 1)
            self.assertIsNone(REAL_ID_RE.search(captured[0]))

    def test_single_shot_counts_the_exact_markdown_it_sends(self) -> None:
        with _temporary_engine("cm_advance_token_parity_") as eng:
            root = eng.root_bucket_id()
            _write_memory(eng, root, "root-memory")
            counted: list[str] = []
            sent: list[tuple[str, str]] = []

            def _count(text: str) -> int:
                counted.append(text)
                return 1

            async def _fake_request(*, system_text: str, user_markdown: str, **_kwargs):
                sent.append((system_text, user_markdown))
                return Prompts(TextPrompt("assistant", "ok"))

            async def _run() -> None:
                svc = eng._advance
                with (
                    patch.object(svc, "_advance_count_tokens_exact", side_effect=_count),
                    patch.object(svc, "_advance_llm_request", side_effect=_fake_request),
                ):
                    await eng.advance_query(command="inspect", mode="single_shot", enable_aliasing=True)

            asyncio.run(_run())
            self.assertEqual(len(counted), 1)
            self.assertEqual(len(sent), 1)
            self.assertEqual(
                counted[0],
                eng._advance._advance_combine_request_markdown(
                    system_text=sent[0][0],
                    user_markdown=sent[0][1],
                ),
            )

    def test_fail_closed_prevents_llm_call_when_real_id_survives(self) -> None:
        with _temporary_engine("cm_advance_fail_closed_") as eng:
            root = eng.root_bucket_id()
            mem_id = _write_memory(eng, root, "root-memory")
            calls = 0

            async def _fake_request(**_kwargs):
                nonlocal calls
                calls += 1
                return Prompts(TextPrompt("assistant", "unexpected"))

            async def _unsafe_prepare(_bucket_id, value, **_kwargs):
                return value

            async def _run() -> None:
                svc = eng._advance
                with (
                    patch.object(eng._alias, "prepare_alias_payload", side_effect=_unsafe_prepare),
                    patch.object(svc, "_advance_llm_request", side_effect=_fake_request),
                    patch.object(svc, "_advance_count_tokens_exact", return_value=1),
                ):
                    with self.assertRaises(AliasPayloadError):
                        await eng.advance_query(command=f"inspect {mem_id}", mode="single_shot", enable_aliasing=True)

            asyncio.run(_run())
            self.assertEqual(calls, 0)


if __name__ == "__main__":
    unittest.main()
