Return JSON only.

Schema:
{
  "accept": true,
  "reject_reason": "string, empty when accept=true",
  "input_type": "plain|json|chat_event|log|source_code|unknown",
  "skip_clean": false,
  "preserve_literal": false,
  "clean_text": "string, keep concise; when skip_clean=true prefer empty string",
  "memory_doc": {
    "source": "string",
    "content": "string"
  }
}
