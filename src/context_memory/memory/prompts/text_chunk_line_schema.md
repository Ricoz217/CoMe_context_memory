Return JSON only.

Schema:
{
  "chunks": [
    {
      "chunk_id": "string",
      "ranges": [[1, 10], [15, 20]],
      "replacements": {
        "3": "new line content",
        "8": "inserted line A\\ninserted line B"
      },
      "note": "optional short reason"
    }
  ]
}

Validation constraints:
1. `chunks` must be non-empty for valid split output.
2. `ranges` must be non-empty per chunk.
3. Every range item must be `[start_line, end_line]` integers.
4. `replacements` keys are line numbers as strings.
5. `replacements` values are strings.
6. `note` must be short (<=120 chars) and must not contain long source text.
7. Do not include extra fields containing reconstructed/raw chunk content.
