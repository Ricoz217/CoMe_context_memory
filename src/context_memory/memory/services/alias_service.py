from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

from ..aliasing import AliasPayloadError, AliasTable, stable_payload_hash

if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class AliasService:
    def __init__(self, runtime: "EngineRuntime", *, topology: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._topology_provider = topology

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    def next_request_id(self, tool: str) -> str:
        self.runtime._alias_request_seq += 1
        seq = self.runtime._alias_request_seq
        now = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        return f'{tool}_{now}_{seq}'

    def begin_session(self) -> None:
        begin = getattr(self.runtime.storage, 'begin_alias_session', None)
        if callable(begin):
            begin()

    def end_session(self, *, flush: bool = True) -> None:
        end = getattr(self.runtime.storage, 'end_alias_session', None)
        if callable(end):
            end(flush=flush)

    async def audit_llm_call(
        self,
        *,
        tool: str,
        bucket_id: str,
        map_version: int,
        alias_input: Any,
        alias_output: Any,
    ) -> None:
        await self.runtime.run_storage_task(
            self._append_llm_audit_sync,
            tool=tool,
            bucket_id=bucket_id,
            map_version=map_version,
            alias_input=alias_input,
            alias_output=alias_output,
        )

    def _append_llm_audit_sync(
        self,
        *,
        tool: str,
        bucket_id: str,
        map_version: int,
        alias_input: Any,
        alias_output: Any,
    ) -> None:
        self.runtime.storage.append_alias_audit(
            {
                'request_id': self.next_request_id(tool),
                'bucket_id': bucket_id,
                'map_version': int(map_version),
                'tool': tool,
                'input_hash': stable_payload_hash(alias_input),
                'output_hash': stable_payload_hash(alias_output),
                'alias_map_hash': self._alias_table(bucket_id).snapshot_hash(),
            }
        )

    def _alias_table(self, bucket_id: str, *, resolve_successor: bool=True) -> AliasTable:
        token = str(bucket_id or '').strip()
        if not token:
            raise ValueError('bucket_id is empty')
        resolved = self.topology._resolve_bucket_id(token) if resolve_successor else token
        return self.runtime.alias_codec.store.open(resolved)

    def get_or_create_alias(self, bucket_id: str, real_key: str, key_type: str) -> str:
        return self._alias_table(bucket_id).to_alias(real_key, key_type=key_type)

    def resolve_alias(self, bucket_id: str, alias: str, expected_type: str | None=None) -> str:
        return self._alias_table(bucket_id).to_real(alias, expected_type=expected_type)

    async def _resolve_alias_from_resolved_bucket(self, bucket_id: str, alias: str, *, expected_type: str | None=None) -> str:
        table = self.runtime.alias_codec.store.open(bucket_id)
        return await self.runtime.run_storage_task(table.to_real, alias, expected_type=expected_type)

    async def _resolve_aliases_from_resolved_bucket(self, bucket_id: str, aliases: Iterable[str], *, expected_type: str | None=None, strict: bool=False) -> dict[str, str]:
        table = self.runtime.alias_codec.store.open(bucket_id)
        alias_batch = tuple(aliases)
        return await self.runtime.run_storage_task(table.to_real_many, alias_batch, expected_type=expected_type, strict=strict)

    async def resolve_aliases(self, bucket_id: str, aliases: Iterable[str], *, expected_type: str | None=None, strict: bool=False) -> dict[str, str]:
        """Resolve aliases in one bucket; invalid entries are skipped unless strict."""
        resolved = self.topology._resolve_bucket_id(bucket_id)
        return await self._resolve_aliases_from_resolved_bucket(resolved, aliases, expected_type=expected_type, strict=strict)

    def freeze_alias_map(self, bucket_id: str) -> None:
        self._alias_table(bucket_id).freeze()

    def alias_map_version(self, bucket_id: str) -> int:
        return self._alias_table(bucket_id).map_version()

    def build_llm_view(self, bucket_id: str, real_payload: Any, map_version: int | None=None, *, allow_create: bool=True) -> Any:
        resolved = self.topology._resolve_bucket_id(bucket_id)
        return self.runtime.alias_codec.build_llm_view(resolved, real_payload, map_version=map_version, allow_create=allow_create)

    async def prepare_alias_payload(self, bucket_id: str, real_payload: Any, map_version: int | None=None, *, allow_create: bool=True) -> Any:
        """Build and persist an alias-only payload without blocking the event loop."""
        prepared, _ = await self._prepare_alias_payload_with_version(bucket_id, real_payload, allow_create=allow_create, map_version=map_version)
        return prepared

    async def _prepare_alias_payload_with_version(self, bucket_id: str, real_payload: Any, map_version: int | None=None, *, allow_create: bool=True) -> tuple[Any, int]:
        table = self._alias_table(bucket_id)
        return await self.runtime.run_storage_task(table.encode_tree_with_version, real_payload, allow_create=allow_create, map_version=map_version)

    async def restore_alias_payload(self, bucket_id: str, alias_payload: Any, map_version: int | None=None, *, strict_unknown: bool=True) -> Any:
        """Restore a structured LLM response through the bucket's AliasTable."""
        table = self._alias_table(bucket_id)
        return await self.runtime.run_storage_task(table.decode_tree, alias_payload, map_version=map_version, strict_unknown=strict_unknown)

    async def _assert_alias_payload_safe(self, bucket_id: str, payload: Any) -> None:
        table = self._alias_table(bucket_id)
        await self.runtime.run_storage_task(table.assert_safe, payload)

    def resolve_llm_output(self, bucket_id: str, alias_output: Any, map_version: int | None=None) -> Any:
        resolved = self.topology._resolve_bucket_id(bucket_id)
        return self.runtime.alias_codec.resolve_llm_output(resolved, alias_output, map_version=map_version)

    def assert_alias_only_payload(self, bucket_id: str, payload: Any) -> None:
        resolved = self.topology._resolve_bucket_id(bucket_id)
        try:
            self.runtime.alias_codec.assert_alias_only_payload(resolved, payload)
        except AliasPayloadError as exc:
            self.runtime.storage.append_alias_audit({'request_id': self.next_request_id('failfast'), 'tool': 'failfast', 'bucket_id': resolved, 'message': str(exc), 'input_hash': stable_payload_hash(payload)})
            raise
