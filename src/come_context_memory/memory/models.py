from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


RELATION_ALLOWED_TYPES: dict[str, set[str]] = {
    "entity_links": {"about", "actor", "owner", "member_of", "mentions"},
    "memory_links": {"supports", "extends", "duplicates", "references"},
    "temporal_links": {"before", "after", "overlaps", "same_period"},
    "causal_links": {"causes", "caused_by", "enables", "blocks"},
    "dependency_links": {"depends_on", "required_by", "prerequisite_of"},
    "evidence_links": {"derived_from", "corroborates", "source_of"},
    "conflict_links": {"contradicts", "disputed_by", "mutually_exclusive"},
    "lifecycle_links": {"supersedes", "superseded_by", "revises", "tombstones"},
}

RELATION_KEYS: tuple[str, ...] = tuple(RELATION_ALLOWED_TYPES.keys())

BUCKET_KIND_MEMORY = "memory"
BUCKET_KIND_BUCKET = "bucket"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_or_none(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def empty_relations() -> dict[str, list[dict[str, Any]]]:
    return {k: [] for k in RELATION_KEYS}


def normalize_relations(raw: Any) -> dict[str, list[dict[str, Any]]]:
    normalized = empty_relations()
    if not isinstance(raw, dict):
        return normalized

    for category in RELATION_KEYS:
        values = raw.get(category, [])
        if not isinstance(values, list):
            continue
        allowed = RELATION_ALLOWED_TYPES[category]
        out: list[dict[str, Any]] = []
        for item in values:
            if not isinstance(item, dict):
                continue
            target = str(item.get("target", "")).strip()
            rel_type = str(item.get("type", "")).strip()
            if not target or rel_type not in allowed:
                continue
            try:
                score = float(item.get("score", 0.5))
            except (TypeError, ValueError):
                score = 0.5
            score = max(0.0, min(1.0, score))
            clean = {"target": target, "type": rel_type, "score": score}
            note = item.get("note")
            if isinstance(note, str) and note.strip():
                clean["note"] = note.strip()
            out.append(clean)
        normalized[category] = out
    return normalized


@dataclass(slots=True)
class BucketInfo:
    bucket_id: str
    parent_bucket_id: str | None
    level: int
    title: str
    summary: str
    node_key: str
    summary_status: str = "ready"
    summary_locked: bool = False
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    children: list[str] = field(default_factory=list)
    sealed: bool = False
    sealed_to: str = ""
    archived: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BucketInfo":
        children = data.get("children", [])
        if not isinstance(children, list):
            children = []
        return cls(
            bucket_id=str(data.get("bucket_id", "")),
            parent_bucket_id=(str(data.get("parent_bucket_id")) if data.get("parent_bucket_id") is not None else None),
            level=int(data.get("level", 1)),
            title=str(data.get("title", "")),
            summary=str(data.get("summary", "")),
            node_key=str(data.get("node_key", "")),
            summary_status=str(data.get("summary_status", "ready") or "ready"),
            summary_locked=bool(data.get("summary_locked", False)),
            created_at=str(data.get("created_at", utc_now_iso())),
            updated_at=str(data.get("updated_at", utc_now_iso())),
            children=[str(c) for c in children if str(c).strip()],
            sealed=bool(data.get("sealed", False)),
            sealed_to=str(data.get("sealed_to", "")),
            archived=bool(data.get("archived", False)),
        )


@dataclass(slots=True)
class MemoryRecord:
    key: str
    revision_id: str
    kind: str
    bucket_id: str
    title: str
    summary: str
    content: str
    weight: float
    event: str
    gray: bool
    relations: dict[str, list[dict[str, Any]]] = field(default_factory=empty_relations)
    evidence_ref: str = ""
    created_at: str = field(default_factory=utc_now_iso)
    expires_at: str | None = None
    source_hash: str = ""
    child_bucket_id: str = ""
    evidence_content: str = ""

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["relations"] = normalize_relations(self.relations)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MemoryRecord":
        return cls(
            key=str(data.get("key", "")),
            revision_id=str(data.get("revision_id", "")),
            kind=str(data.get("kind", BUCKET_KIND_MEMORY)),
            bucket_id=str(data.get("bucket_id", "")),
            title=str(data.get("title", "")),
            summary=str(data.get("summary", "")),
            content=str(data.get("content", "")),
            weight=float(data.get("weight", 0.5)),
            event=str(data.get("event", "")),
            gray=bool(data.get("gray", False)),
            relations=normalize_relations(data.get("relations", {})),
            evidence_ref=str(data.get("evidence_ref", "")),
            created_at=str(data.get("created_at", utc_now_iso())),
            expires_at=data.get("expires_at"),
            source_hash=str(data.get("source_hash", "")),
            child_bucket_id=str(data.get("child_bucket_id", "")),
            evidence_content=str(data.get("evidence_content", "")),
        )


@dataclass(slots=True)
class MemorySnapshot:
    key: str
    revision_id: str
    kind: str
    bucket_id: str
    title: str
    summary: str
    content: str
    weight: float
    gray: bool
    child_bucket_id: str = ""

    @classmethod
    def from_record(cls, record: MemoryRecord) -> "MemorySnapshot":
        return cls(
            key=record.key,
            revision_id=record.revision_id,
            kind=record.kind,
            bucket_id=record.bucket_id,
            title=record.title,
            summary=record.summary,
            content=record.content,
            weight=record.weight,
            gray=record.gray,
            child_bucket_id=record.child_bucket_id,
        )


@dataclass(slots=True)
class QueryMatch:
    key: str
    score: float
    reason: str
    summary: str
    source: str = "llm"
    llm_score: float = 0.0
    bm25_score: float = 0.0
    final_score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueryMatch":
        return cls(
            key=str(data.get("key", "")),
            score=float(data.get("score", 0.0)),
            reason=str(data.get("reason", "")),
            summary=str(data.get("summary", "")),
            source=str(data.get("source", "llm")),
            llm_score=float(data.get("llm_score", 0.0)),
            bm25_score=float(data.get("bm25_score", 0.0)),
            final_score=float(data.get("final_score", data.get("score", 0.0))),
        )


@dataclass(slots=True)
class AddResult:
    success: bool
    key: str = ""
    revision_id: str = ""
    message: str = ""
    added_keys: list[str] = field(default_factory=list)
    split_performed: bool = False
    split_rebuild_detected: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class UpdateResult:
    success: bool
    key: str = ""
    revision_id: str = ""
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DeleteResult:
    success: bool
    key: str = ""
    revision_id: str = ""
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class QueryResult:
    success: bool
    answer: str = ""
    matches: list[QueryMatch] = field(default_factory=list)
    result_source: str = "LOCAL"
    cache_hit: bool = False
    include_gray_used: bool = True
    degraded: bool = False
    degraded_reason: str = ""
    failure_stage: str = ""
    sub_answer: str = ""
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "answer": self.answer,
            "matches": [m.to_dict() for m in self.matches],
            "result_source": self.result_source,
            "cache_hit": self.cache_hit,
            "include_gray_used": self.include_gray_used,
            "degraded": self.degraded,
            "degraded_reason": self.degraded_reason,
            "failure_stage": self.failure_stage,
            "sub_answer": self.sub_answer,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueryResult":
        raw_matches = data.get("matches", [])
        matches: list[QueryMatch] = []
        if isinstance(raw_matches, list):
            for item in raw_matches:
                if isinstance(item, dict):
                    matches.append(QueryMatch.from_dict(item))
        return cls(
            success=bool(data.get("success", False)),
            answer=str(data.get("answer", "")),
            matches=matches,
            result_source=str(data.get("result_source", "LOCAL") or "LOCAL"),
            cache_hit=bool(data.get("cache_hit", False)),
            include_gray_used=bool(data.get("include_gray_used", True)),
            degraded=bool(data.get("degraded", False)),
            degraded_reason=str(data.get("degraded_reason", "")),
            failure_stage=str(data.get("failure_stage", "")),
            sub_answer=str(data.get("sub_answer", "")),
            message=str(data.get("message", "")),
        )


@dataclass(slots=True)
class CompressResult:
    success: bool
    changed: int = 0
    dropped: int = 0
    reweighted: int = 0
    rewritten: int = 0
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OptimizeResult:
    success: bool
    bucket_id: str = ""
    message: str = ""
    reason_code: str = ""
    coverage_ratio: float = 0.0
    skipped_invalid_count: int = 0
    created_buckets: list[str] = field(default_factory=list)
    moved_items: int = 0
    sealed_redirects: dict[str, str] = field(default_factory=dict)
    post_actions: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class MoveResult:
    success: bool
    key: str = ""
    from_bucket: str = ""
    to_bucket: str = ""
    revision_id: str = ""
    moved_kind: str = ""
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class GCResult:
    success: bool
    dry_run: bool = True
    message: str = ""
    deleted: dict[str, int] = field(default_factory=dict)
    would_delete: dict[str, int] = field(default_factory=dict)
    skipped: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CleanupResult:
    success: bool
    expired_marked: int = 0
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EngineStats:
    total_keys: int
    active_keys: int
    gray_keys: int
    revision_total: int
    event_total: int
    cache_entries: int
    dirty: bool
    context_version: int
    estimated_tokens: int
    latest_snapshot: str
    llm_calls_total: int
    llm_input_tokens_total: int
    llm_output_tokens_total: int
    llm_cached_input_tokens_total: int
    llm_cache_hit_rate_global: float
    degraded_query_total: int
    llm_parse_fail_total: int
    llm_precheck_fail_total: int
    clean_reject_total: int
    clean_fallback_total: int
    ingest_blocked_by_clean_total: int
    root_bucket_id: str
    active_bucket_id: str
    bucket_total: int
    memory_cache_bytes: int
    aggressive_memory_mode: bool
    memory_idle_evictions_total: int
    memory_pressure_evictions_total: int
    memory_cleanup_runs_total: int
    memory_aggressive_enters_total: int
    memory_aggressive_seconds_total: float
    context_overflow_total: int
    overflow_query_total: int
    overflow_ingest_total: int
    overflow_compress_total: int
    file_import_reject_total: int
    auto_split_guard_hit_total: int = 0
    auto_split_cooldown_skip_total: int = 0
    auto_split_no_progress_total: int = 0
    split_plan_warn_total: int = 0
    last_auto_split_at: str = ""
    last_split_source_bucket_id: str = ""
    last_split_successor_bucket_id: str = ""
    real_key_leak_count: int = 0
    alias_resolve_fail_count: int = 0
    unknown_alias_count: int = 0
    query_alias_miss_build_total: int = 0
    query_alias_miss_resolve_total: int = 0
    query_side_effect_drop_total: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
