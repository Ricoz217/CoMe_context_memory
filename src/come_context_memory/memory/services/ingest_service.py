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
        image_extract_hint: str = "",
        query_hint: str | None = None,
        force_split: bool = True,
        create_new_bucket: bool = False,
        chunk_max_chars: int | None = None,
        chunk_overlap_chars: int | None = None,
        dedup_in_bucket: bool = True,
        auto_optimize_after_split: bool = True,
    ) -> AddResult:
        eng = self.runtime.engine
        effective_image_hint = str(image_extract_hint or "").strip() or str(query_hint or "").strip()
        path = Path(file_path)
        if not path.exists() or not path.is_file():
            return AddResult(success=False, message=f"file not found: {file_path}")
        target_bucket = eng._resolve_bucket_id(bucket_id)
        before_bucket_count = len(eng.storage.list_buckets())
        kind = detect_file_kind(path)
        result: AddResult
        if kind == "text":
            text = read_text_file(path, max_chars=eng._max_memory_chars * 2)
            if not text.strip():
                return AddResult(success=False, message="text file is empty or unreadable")
            result = await eng.add_memory(
                text,
                evidence_path=str(path),
                topic=topic or path.name,
                bucket_id=target_bucket,
                force_split=force_split,
                create_new_bucket=create_new_bucket,
                chunk_max_chars=chunk_max_chars,
                chunk_overlap_chars=chunk_overlap_chars,
                dedup_in_bucket=dedup_in_bucket,
            )
        elif kind == "image":
            extracted = await eng.image_extractor.extract(path, query=effective_image_hint)
            if not extracted.strip():
                eng.storage.record_file_import_reject()
                return AddResult(success=False, message="image extraction returned empty text")
            result = await eng.add_memory(
                extracted,
                evidence_path=str(path),
                topic=topic or path.name,
                bucket_id=target_bucket,
                force_split=force_split,
                create_new_bucket=create_new_bucket,
                chunk_max_chars=chunk_max_chars,
                chunk_overlap_chars=chunk_overlap_chars,
                dedup_in_bucket=dedup_in_bucket,
            )
        else:
            eng.storage.record_file_import_reject()
            suffix = path.suffix or "(no suffix)"
            return AddResult(success=False, message=f"unsupported file kind: {suffix}; detect_file_kind=unknown")

        if not result.success:
            return result

        after_bucket = eng._resolve_bucket_id(target_bucket)
        after_bucket_count = len(eng.storage.list_buckets())
        split_rebuild_detected = bool(
            result.split_performed
            or after_bucket_count > before_bucket_count
            or after_bucket != target_bucket
        )
        result.split_rebuild_detected = split_rebuild_detected

        if auto_optimize_after_split and split_rebuild_detected and result.added_keys:
            await eng.optimize(bucket_id=after_bucket, reason="auto_post_file_split")
        return result
