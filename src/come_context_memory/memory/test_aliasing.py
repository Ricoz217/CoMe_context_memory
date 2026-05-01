from __future__ import annotations

import unittest

from aliasing import AliasCodec, AliasPayloadError


class _MockStorage:
    def __init__(self) -> None:
        self.maps: dict[str, dict[str, str]] = {}
        self.metrics = {
            "real_key_leak_count": 0,
            "alias_resolve_fail_count": 0,
            "unknown_alias_count": 0,
        }

    def _ensure(self, bucket_id: str) -> dict[str, str]:
        if bucket_id not in self.maps:
            self.maps[bucket_id] = {}
        return self.maps[bucket_id]

    def get_or_create_alias(self, bucket_id: str, real_key: str, key_type: str) -> str:
        m = self._ensure(bucket_id)
        typed = f"{key_type}:{real_key}"
        if typed in m:
            return m[typed]
        seq = 1 + len([k for k in m if k.startswith(f"{key_type}:")])
        alias = f"{key_type}_{seq}"
        m[typed] = alias
        m[alias] = real_key
        return alias

    def resolve_alias(self, bucket_id: str, alias: str, expected_type: str | None = None) -> str:
        m = self._ensure(bucket_id)
        if alias not in m:
            raise KeyError(alias)
        if expected_type and not alias.startswith(f"{expected_type}_"):
            raise TypeError(alias)
        return m[alias]

    def alias_map_version(self, bucket_id: str) -> int:
        return 1

    def freeze_alias_map(self, bucket_id: str) -> None:
        return None

    def record_alias_real_key_leak(self) -> None:
        self.metrics["real_key_leak_count"] += 1

    def record_alias_resolve_fail(self) -> None:
        self.metrics["alias_resolve_fail_count"] += 1

    def record_unknown_alias(self) -> None:
        self.metrics["unknown_alias_count"] += 1


class AliasCodecTests(unittest.TestCase):
    def test_roundtrip(self) -> None:
        st = _MockStorage()
        codec = AliasCodec(st)
        payload = {
            "key": "mem_20260429000000_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "relations": {
                "memory_links": [
                    {
                        "target": "mem_20260429000000_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "type": "references",
                        "score": 1.0,
                    }
                ]
            },
        }
        alias_payload = codec.build_llm_view("b1", payload)
        self.assertTrue(str(alias_payload["key"]).startswith("memory_"))
        out = codec.resolve_llm_output("b1", alias_payload)
        self.assertEqual(out["key"], payload["key"])

    def test_failfast_leak(self) -> None:
        st = _MockStorage()
        codec = AliasCodec(st)
        with self.assertRaises(AliasPayloadError):
            codec.assert_alias_only_payload(
                "b1",
                {"key_hints": ["mem_20260429000000_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
            )
        self.assertEqual(st.metrics["real_key_leak_count"], 1)

    def test_strict_output_rejects_real_key(self) -> None:
        st = _MockStorage()
        codec = AliasCodec(st)
        with self.assertRaises(AliasPayloadError):
            codec.resolve_llm_output(
                "b1",
                {"matches": [{"key": "mem_20260429000000_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]},
            )


if __name__ == "__main__":
    unittest.main()
