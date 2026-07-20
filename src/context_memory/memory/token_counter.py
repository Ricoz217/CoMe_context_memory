from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Literal

import tiktoken


TOKEN_ENCODING = "o200k_base"
FALLBACK_CHARS_PER_TOKEN = 3.0


class TokenCountError(RuntimeError):
    pass


@dataclass(slots=True, frozen=True)
class TokenCount:
    tokens: int
    method: Literal["tiktoken", "char_estimate"]


class TokenCounter:
    """Pure token counting with explicit strict and display policies."""

    def count_text(self, text: str) -> int:
        try:
            encoding = tiktoken.get_encoding(TOKEN_ENCODING)
            return max(1, len(encoding.encode(str(text))))
        except Exception as exc:
            raise TokenCountError(f"failed to count tokens with {TOKEN_ENCODING}") from exc

    def count_json(self, payload: Any, *, sort_keys: bool = True) -> int:
        text = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=sort_keys,
            separators=(",", ":"),
        )
        return self.count_text(text)

    def count_json_with_token_field(
        self,
        payload: dict[str, Any],
        *,
        field_name: str = "payload_tokens",
        sort_keys: bool = True,
    ) -> int:
        """Count the final JSON while making its token-count field self-consistent."""
        prepared = dict(payload)
        tokens = 1
        for _ in range(8):
            prepared[field_name] = tokens
            counted = self.count_json(prepared, sort_keys=sort_keys)
            if counted == tokens:
                return counted
            tokens = counted
        raise TokenCountError(f"failed to stabilize JSON token field: {field_name}")

    def count_text_for_display(self, text: str) -> TokenCount:
        try:
            return TokenCount(tokens=self.count_text(text), method="tiktoken")
        except TokenCountError:
            estimate = max(1, math.ceil(len(str(text)) / FALLBACK_CHARS_PER_TOKEN))
            return TokenCount(tokens=estimate, method="char_estimate")
