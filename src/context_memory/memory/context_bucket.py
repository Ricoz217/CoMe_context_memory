from __future__ import annotations

from pathlib import Path
from typing import Any

from .storage import MemoryStorageV3


class ContextBucketFacade:
    """Light wrapper for bucket-centric operations in V3."""

    def __init__(self, base_dir: str | Path, *, evidence_versions: int = 5) -> None:
        self.storage = MemoryStorageV3(base_dir, evidence_versions=evidence_versions)

    def root_bucket_id(self) -> str:
        return self.storage.get_root_bucket_id()

    def active_bucket_id(self) -> str:
        return self.storage.get_active_bucket_id()

    def load_context(self, bucket_id: str) -> Any:
        return self.storage.load_bucket_context(bucket_id)

    def append_event(self, bucket_id: str, event: dict[str, Any]) -> None:
        self.storage.append_bucket_event(bucket_id, event)

    def list_buckets(self) -> list[dict[str, Any]]:
        return [b.to_dict() for b in self.storage.list_buckets()]
