from __future__ import annotations

import math
import re
from collections import Counter, OrderedDict
from dataclasses import dataclass
from typing import Iterable

try:
    import networkx as nx
    from community import community_louvain

    _HAS_LOUVAIN = True
except Exception:
    nx = None  # type: ignore
    community_louvain = None  # type: ignore
    _HAS_LOUVAIN = False

from .models import MemoryRecord, utc_now_iso


TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]")


def tokenize(text: str) -> list[str]:
    return [t.lower() for t in TOKEN_PATTERN.findall(text or "")]


class BM25Lite:
    def __init__(self, docs: list[list[str]], *, k1: float = 1.5, b: float = 0.75) -> None:
        self.docs = docs
        self.k1 = k1
        self.b = b
        self.doc_count = len(docs)
        self.doc_len = [len(d) for d in docs]
        self.avg_doc_len = (sum(self.doc_len) / self.doc_count) if self.doc_count else 0.0
        self.df: Counter[str] = Counter()
        for doc in docs:
            for term in set(doc):
                self.df[term] += 1
        self.tf: list[Counter[str]] = [Counter(doc) for doc in docs]

    def score(self, query_terms: list[str], index: int) -> float:
        if not self.docs or not query_terms:
            return 0.0
        score = 0.0
        tf = self.tf[index]
        dl = self.doc_len[index]
        if self.avg_doc_len > 0:
            denom_base = self.k1 * (1.0 - self.b + self.b * (dl / self.avg_doc_len))
        else:
            denom_base = self.k1
        for term in query_terms:
            f = tf.get(term, 0)
            if f <= 0:
                continue
            n = self.df.get(term, 0)
            idf = math.log(1.0 + (self.doc_count - n + 0.5) / (n + 0.5))
            score += idf * (f * (self.k1 + 1.0)) / (f + denom_base)
        return score


def _record_text(record: MemoryRecord) -> str:
    relation_text_parts: list[str] = []
    for rel_name, rel_items in record.relations.items():
        relation_text_parts.append(rel_name)
        for item in rel_items:
            relation_text_parts.append(str(item.get("target", "")))
            relation_text_parts.append(str(item.get("type", "")))
            relation_text_parts.append(str(item.get("note", "")))
    relation_text = " ".join(relation_text_parts)
    return " ".join([record.key, record.title, record.summary, record.content, relation_text])


@dataclass(slots=True)
class BucketBM25Index:
    bucket_id: str
    bucket_version: int
    keys: list[str]
    bm25: BM25Lite
    docs_tokens: list[list[str]]
    docs_text: list[str]
    built_at: str


class BM25IndexCache:
    def __init__(self, *, max_buckets: int = 32) -> None:
        self.max_buckets = max(1, int(max_buckets))
        self._cache: OrderedDict[str, BucketBM25Index] = OrderedDict()

    def get_or_build(self, *, bucket_id: str, bucket_version: int, records: list[MemoryRecord]) -> BucketBM25Index:
        key = f"{bucket_id}:{bucket_version}"
        if key in self._cache:
            idx = self._cache.pop(key)
            self._cache[key] = idx
            return idx

        docs_text = [_record_text(rec) for rec in records]
        docs_tokens = [tokenize(txt) for txt in docs_text]
        bm25 = BM25Lite(docs_tokens)
        idx = BucketBM25Index(
            bucket_id=bucket_id,
            bucket_version=bucket_version,
            keys=[rec.key for rec in records],
            bm25=bm25,
            docs_tokens=docs_tokens,
            docs_text=docs_text,
            built_at=utc_now_iso(),
        )
        self._cache[key] = idx
        while len(self._cache) > self.max_buckets:
            self._cache.popitem(last=False)
        return idx

    def clear_old_versions(self, *, bucket_id: str, keep_version: int) -> None:
        stale = [k for k in self._cache.keys() if k.startswith(f"{bucket_id}:") and not k.endswith(f":{keep_version}")]
        for k in stale:
            self._cache.pop(k, None)

    def estimate_memory_bytes(self) -> int:
        total = 0
        for idx in self._cache.values():
            total += sum(len(t) for t in idx.docs_text) * 2
            total += sum(len(doc) for doc in idx.docs_tokens) * 16
            total += len(idx.keys) * 64
        return total

    def prune_to_limit(self, *, approx_limit_bytes: int) -> None:
        if approx_limit_bytes <= 0:
            return
        while self._cache and self.estimate_memory_bytes() > approx_limit_bytes:
            self._cache.popitem(last=False)


def rank_records_with_index(
    query: str,
    records: Iterable[MemoryRecord],
    *,
    top_k: int,
    index: BucketBM25Index,
) -> list[tuple[MemoryRecord, float]]:
    record_list = list(records)
    if not record_list:
        return []

    query_terms = tokenize(query)
    ranked: list[tuple[MemoryRecord, float]] = []
    query_lower = query.lower()

    key_to_pos = {k: i for i, k in enumerate(index.keys)}
    for rec in record_list:
        pos = key_to_pos.get(rec.key)
        if pos is None:
            score = 0.0
        else:
            score = index.bm25.score(query_terms, pos)

        title_lower = rec.title.lower()
        summary_lower = rec.summary.lower()
        content_lower = rec.content.lower()
        if query_lower and query_lower in title_lower:
            score += 2.0
        if query_lower and query_lower in summary_lower:
            score += 1.2
        if query_lower and query_lower in content_lower:
            score += 1.0
        score += max(0.0, min(1.0, float(rec.weight))) * 0.2
        ranked.append((rec, score))

    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked[: max(1, top_k)]


def normalize_scores(scores: list[float]) -> list[float]:
    if not scores:
        return []
    lo = min(scores)
    hi = max(scores)
    if hi - lo < 1e-12:
        return [1.0 if s > 0 else 0.0 for s in scores]
    return [max(0.0, min(1.0, (s - lo) / (hi - lo))) for s in scores]


def louvain_split_groups(records: list[MemoryRecord], *, target_groups_min: int = 2, target_groups_max: int = 10) -> list[list[MemoryRecord]]:
    if not records:
        return []

    if not _HAS_LOUVAIN or len(records) < 4:
        return _balanced_split(records, groups=min(max(target_groups_min, 2), max(2, min(target_groups_max, len(records)))))

    docs_tokens = [set(tokenize(_record_text(r))) for r in records]
    graph = nx.Graph()  # type: ignore
    for idx, rec in enumerate(records):
        graph.add_node(rec.key, idx=idx)

    for i in range(len(records)):
        for j in range(i + 1, len(records)):
            a = docs_tokens[i]
            b = docs_tokens[j]
            if not a or not b:
                continue
            inter = len(a.intersection(b))
            if inter <= 0:
                continue
            union = len(a.union(b))
            if union <= 0:
                continue
            sim = inter / union
            if sim > 0.03:
                graph.add_edge(records[i].key, records[j].key, weight=sim)

    if graph.number_of_edges() <= 0:
        return _balanced_split(records, groups=min(max(target_groups_min, 2), max(2, min(target_groups_max, len(records)))))

    partition: dict[str, int] = community_louvain.best_partition(graph, weight="weight")  # type: ignore
    groups_map: dict[int, list[MemoryRecord]] = {}
    key_to_record = {r.key: r for r in records}
    for key, gid in partition.items():
        rec = key_to_record.get(key)
        if rec is None:
            continue
        groups_map.setdefault(int(gid), []).append(rec)

    groups = [g for g in groups_map.values() if g]
    if len(groups) < target_groups_min:
        return _balanced_split(records, groups=min(max(target_groups_min, 2), max(2, min(target_groups_max, len(records)))))

    while len(groups) > target_groups_max:
        groups.sort(key=len)
        left = groups.pop(0)
        groups[0].extend(left)

    return groups


def _balanced_split(records: list[MemoryRecord], *, groups: int) -> list[list[MemoryRecord]]:
    groups = max(1, min(groups, len(records)))
    out: list[list[MemoryRecord]] = [[] for _ in range(groups)]
    ordered = sorted(records, key=lambda r: len(r.content), reverse=True)
    for idx, rec in enumerate(ordered):
        out[idx % groups].append(rec)
    return [g for g in out if g]
