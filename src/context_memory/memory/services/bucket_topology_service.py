from __future__ import annotations

import asyncio

from ..models import BucketInfo
from .runtime import ServiceRuntime


class BucketTopologyService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    async def resolve_bucket_handle_id(self, bucket_id: str) -> str:
        eng = self.runtime.engine
        return await asyncio.to_thread(eng._resolve_bucket_id, bucket_id)

    def get_bucket(self, bucket_id: str):
        eng = self.runtime.engine
        canonical, _ = eng._resolve_bucket_redirect_chain(bucket_id)
        return eng._bucket_handle_cls(eng, canonical)

    def list_buckets(self) -> list[BucketInfo]:
        eng = self.runtime.engine
        root = eng.root_bucket_id()
        active = eng.active_bucket_id()
        infos = eng.storage.list_buckets()
        infos.sort(key=lambda x: (x.level, x.bucket_id))
        return infos
