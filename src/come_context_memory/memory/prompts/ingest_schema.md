Return JSON only.

Allowed relation categories and types:
- entity_links: about | actor | owner | member_of | mentions
- memory_links: supports | extends | duplicates | references
- temporal_links: before | after | overlaps | same_period
- causal_links: causes | caused_by | enables | blocks
- dependency_links: depends_on | required_by | prerequisite_of
- evidence_links: derived_from | corroborates | source_of
- conflict_links: contradicts | disputed_by | mutually_exclusive
- lifecycle_links: supersedes | superseded_by | revises | tombstones

Schema:
{
  "kind": "memory",
  "title": "string",
  "summary": "string",
  "weight": 0.0,
  "event": "ADD|UPDATE|GRAY_SET|GRAY_CLEAR|COMPRESS_REWEIGHT|COMPRESS_REWRITE",
  "gray": false,
  "expires_at": "ISO8601 string or null",
  "relations": {
    "entity_links": [{"target":"string","type":"about","score":0.0,"note":"optional"}],
    "memory_links": [],
    "temporal_links": [],
    "causal_links": [],
    "dependency_links": [],
    "evidence_links": [],
    "conflict_links": [],
    "lifecycle_links": []
  }
}
