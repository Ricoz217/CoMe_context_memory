from __future__ import annotations

from pathlib import Path

import pytest

from come_context_memory import ContextMemoryConfig, ContextMemoryEngineV3
from come_context_memory.memory.services.query_service import QueryService
from come_context_memory.rpc_server import RpcError, _call


@pytest.mark.asyncio
async def test_query_literal_mode_rejected(tmp_path: Path) -> None:
    engine = ContextMemoryEngineV3(
        config=ContextMemoryConfig(
            base_dir=tmp_path / "store_literal_reject",
            use_mock_llm=True,
            auto_manage=False,
        )
    )
    with pytest.raises(ValueError, match="literal"):
        await engine.query("cache write", mode="literal")


def test_query_mode_default_literal_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="query_mode_default"):
        ContextMemoryEngineV3(
            config=ContextMemoryConfig(
                base_dir=tmp_path / "store_default_reject",
                use_mock_llm=True,
                auto_manage=False,
                query_mode_default="literal",
            )
        )


def test_query_mode_auto_routes_to_semantic_or_hybrid() -> None:
    assert QueryService._resolve_query_mode("auto", "how to write cache metadata", "auto") == "semantic"
    assert QueryService._resolve_query_mode("auto", "def add_file(file: bytes):", "auto") == "hybrid"
    assert QueryService._resolve_query_mode("hybrid", "plain text query", "auto") == "hybrid"


def test_ngram_similarity_prefers_literal_overlap() -> None:
    q = QueryService._char_ngrams("def add_file(file: bytes) -> str", n=3)
    same = QueryService._char_ngrams("def add_file(file: str) -> str", n=3)
    diff = QueryService._char_ngrams("network retry timeout proxy setup", n=3)
    assert QueryService._dice_score(q, same) > QueryService._dice_score(q, diff)


@pytest.mark.asyncio
async def test_jsonrpc_query_literal_mode_rejected(tmp_path: Path) -> None:
    engine = ContextMemoryEngineV3(
        config=ContextMemoryConfig(
            base_dir=tmp_path / "store_rpc_literal_reject",
            use_mock_llm=True,
            auto_manage=False,
        )
    )
    with pytest.raises(RpcError) as exc:
        await _call(
            engine,
            "query",
            {
                "query_text": "cache write",
                "mode": "literal",
            },
        )
    assert exc.value.code == -32602
