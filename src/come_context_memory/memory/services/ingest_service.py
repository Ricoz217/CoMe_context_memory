from __future__ import annotations

from pathlib import Path

from ..models import AddResult
from ..multimodal import detect_file_kind, read_text_file
from .runtime import ServiceRuntime


class IngestService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    async def add_memory_from_file(
        self,
        file_path: str,
        *,
        topic: str = "",
        bucket_id: str | None = None,
        query_hint: str = "",
        force_split: bool = True,
        create_new_bucket: bool = False,
        chunk_max_chars: int | None = None,
        chunk_overlap_chars: int | None = None,
        dedup_in_bucket: bool = True,
    ) -> AddResult:
        eng = self.runtime.engine
        path = Path(file_path)
        if not path.exists() or not path.is_file():
            return AddResult(success=False, message=f"file not found: {file_path}")
        kind = detect_file_kind(path)
        if kind == "text":
            text = read_text_file(path, max_chars=eng._max_memory_chars * 2)
            if not text.strip():
                return AddResult(success=False, message="text file is empty or unreadable")
            return await eng.add_memory(
                text,
                evidence_path=str(path),
                topic=topic or path.name,
                bucket_id=bucket_id,
                force_split=force_split,
                create_new_bucket=create_new_bucket,
                chunk_max_chars=chunk_max_chars,
                chunk_overlap_chars=chunk_overlap_chars,
                dedup_in_bucket=dedup_in_bucket,
            )
        if kind == "image":
            extracted = await eng.image_extractor.extract(path, query=query_hint)
            if not extracted.strip():
                eng.storage.record_file_import_reject()
                return AddResult(success=False, message="image extraction returned empty text")
            return await eng.add_memory(
                extracted,
                evidence_path=str(path),
                topic=topic or path.name,
                bucket_id=bucket_id,
                force_split=force_split,
                create_new_bucket=create_new_bucket,
                chunk_max_chars=chunk_max_chars,
                chunk_overlap_chars=chunk_overlap_chars,
                dedup_in_bucket=dedup_in_bucket,
            )
        eng.storage.record_file_import_reject()
        suffix = path.suffix or "(no suffix)"
        return AddResult(success=False, message=f"unsupported file kind: {suffix}; detect_file_kind=unknown")
