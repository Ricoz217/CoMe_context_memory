from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from context_memory.LLM_connect import Prompts, TextPrompt, ToolInput
from context_memory.memory import engine as memory_engine


def _create_engine(base_dir: str | Path):
    return memory_engine.ContextMemoryEngineV3(
        base_dir=base_dir,
        use_mock_llm=True,
        init_config=False,
        auto_resume_pending_jobs=False,
    )


class TestAdvanceQueryMock(unittest.TestCase):
    def test_single_shot_mock_returns_raw_response(self):
        with tempfile.TemporaryDirectory(prefix="cm_adv_query_mock_") as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                eng = _create_engine(td)

            mocked_response = Prompts(TextPrompt("assistant", "advance_query mock ok"))
            mock_tool = ToolInput(lambda: "ok", "noop")

            with (
                patch.object(eng._advance_query_service, "_advance_payload_tokens", return_value=1),
                patch.object(
                    eng._advance_query_service,
                    "_advance_llm_request",
                    new=AsyncMock(return_value=mocked_response),
                ) as req_mock,
            ):
                out = asyncio.run(
                    eng.advance_query(
                        command="summarize",
                        mode="single_shot",
                        enable_aliasing=False,
                        tool_input=mock_tool,
                    )
                )

            self.assertIs(out, mocked_response)
            self.assertEqual(req_mock.await_count, 1)
            kwargs = req_mock.await_args.kwargs
            self.assertTrue(bool(kwargs.get("allow_tools", False)))
            self.assertIs(kwargs.get("tool_input"), mock_tool)

    def test_best_effort_dispatches_to_best_effort_runner_on_overflow(self):
        with tempfile.TemporaryDirectory(prefix="cm_adv_query_mock_") as td:
            with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
                eng = _create_engine(td)

            mocked_response = Prompts(TextPrompt("assistant", "best effort mock ok"))
            threshold = int(eng.max_context_window * 0.8)

            with (
                patch.object(eng._advance_query_service, "_advance_payload_tokens", return_value=threshold + 1),
                patch.object(
                    eng._advance_query_service,
                    "_advance_run_best_effort_node",
                    new=AsyncMock(return_value=mocked_response),
                ) as best_effort_mock,
            ):
                out = asyncio.run(
                    eng.advance_query(
                        command="analyze",
                        mode="best_effort_full_view",
                        enable_aliasing=False,
                    )
                )

            self.assertIs(out, mocked_response)
            self.assertEqual(best_effort_mock.await_count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
