from __future__ import annotations

from ..models import BucketInfo
from .runtime import ServiceRuntime


class BucketTopologyService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    async def resolve_bucket_handle_id(self, bucket_id: str) -> str:
        eng = self.runtime.engine
        return eng._resolve_bucket_id(bucket_id)

    def get_bucket(self, bucket_id: str):
        eng = self.runtime.engine
        canonical, lineage = eng._resolve_bucket_redirect_chain(bucket_id)
        if canonical != bucket_id:
            old_ids = set(lineage[:-1]) if len(lineage) > 1 else {bucket_id}
            eng._sync_bucket_mapping_redirect(old_ids=old_ids, new_id=canonical)
        return eng._bucket_handle_cls(eng, canonical)

    def list_buckets(self) -> list[BucketInfo]:
        eng = self.runtime.engine
        root = eng.root_bucket_id()
        active = eng.active_bucket_id()
        infos = eng.storage.list_buckets()
        infos.sort(key=lambda x: (x.level, x.bucket_id))
        return infos
