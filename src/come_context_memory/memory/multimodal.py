from __future__ import annotations

from pathlib import Path

from come_context_memory.LLM_connect import Chat, Context, ImagePrompt, Prompts, SystemPrompt, TextPrompt, parse_llm_setting


IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif", ".tif", ".tiff", ".ico"}
TEXT_EXTS = {
    ".txt",
    ".md",
    ".json",
    ".yaml",
    ".yml",
    ".log",
    ".csv",
    ".py",
    ".toml",
    ".ini",
    ".xml",
    ".cfg",
    ".conf",
    ".env",
    ".sql",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".sh",
    ".bat",
    ".ps1",
}


class ImageTextExtractor:
    def __init__(
        self,
        *,
        llm_preset: str = "KIMI2.6",
        max_extract_chars: int = 20_000,
        init_config: bool = True,
        prompt_dir: str | Path | None = None,
    ) -> None:
        self.llm_preset = llm_preset
        self.max_extract_chars = max(500, min(int(max_extract_chars), 100_000))
        self.init_config = init_config
        self.prompt_dir = (
            Path(prompt_dir)
            if prompt_dir is not None
            else Path(__file__).resolve().parent / "prompts"
        )
        self._prompt_cache: dict[str, str] = {}

    def _load_prompt(self, filename: str, fallback: str) -> str:
        cached = self._prompt_cache.get(filename)
        if cached is not None:
            return cached
        path = self.prompt_dir / filename
        if path.exists():
            try:
                text = path.read_text(encoding="utf-8").strip()
                if text:
                    self._prompt_cache[filename] = text
                    return text
            except Exception:
                pass
        self._prompt_cache[filename] = fallback
        return fallback

    def _render_user_prompt(self, query: str) -> str:
        template = self._load_prompt(
            "image_extract_user.md",
            (
                "Extract useful text and key facts from this image.\n"
                "Focus hint: {{query_or_default}}"
            ),
        )
        q = (query or "").strip()
        if not q:
            q = "No extra focus hint. Extract all visible text and key visual facts."
        return (
            template.replace("{{query_or_default}}", q)
            .replace("{{query}}", (query or "").strip())
            .strip()
        )

    def _resolve_prompts(self, query: str) -> tuple[str, str]:
        system_text = self._load_prompt(
            "image_extract_system.md",
            (
                "You are an OCR+VLM extractor for memory ingestion. "
                "Return concise, factual plain text. "
                "Do not output markdown code fences."
            ),
        )
        prompt_text = self._render_user_prompt(query)
        return system_text, prompt_text

    async def extract(self, image_path: str | Path, *, query: str = "") -> str:
        path = Path(image_path)
        if not path.exists() or not path.is_file():
            return ""

        preset_name = str(self.llm_preset or "").strip()
        if not preset_name:
            raise RuntimeError("image llm preset is empty; please set image_llm_preset or tool_presets.image_extract")

        try:
            setting = parse_llm_setting(preset_name)
        except Exception as exc:
            raise RuntimeError(f"parse_llm_setting failed for image preset '{preset_name}': {exc}") from exc
        if setting is None:
            return ""

        system_text, prompt_text = self._resolve_prompts(query)

        chat = None
        result = None
        try:
            chat = Chat(keep_alive=False)
            chat.setting(setting)
            ctx = Context(SystemPrompt(system_text))
            chat.replace_context(ctx)
            result = await chat.ask(
                Prompts(TextPrompt("user", prompt_text), ImagePrompt("user", path)),
                timeout=180,
            )
        except Exception:
            return ""
        finally:
            if chat is not None:
                try:
                    await chat.close()
                except Exception:
                    pass

        if result is None:
            return ""

        texts: list[str] = []
        for p in getattr(result, "prompts", []):
            if getattr(p, "role", "") == "assistant":
                text = str(getattr(p, "text", "") or "").strip()
                if text:
                    texts.append(text)
        out = "\n".join(texts).strip()
        if not out:
            return ""
        return out[: self.max_extract_chars]


def _read_head(path: Path, limit: int = 4096) -> bytes:
    try:
        with path.open("rb") as f:
            return f.read(max(64, int(limit)))
    except Exception:
        return b""


def _is_image_magic(head: bytes) -> bool:
    if len(head) < 4:
        return False
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return True
    if head.startswith(b"\xff\xd8\xff"):
        return True
    if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
        return True
    if head.startswith(b"BM"):
        return True
    if head.startswith(b"II*\x00") or head.startswith(b"MM\x00*"):
        return True
    if head.startswith(b"\x00\x00\x01\x00"):
        return True
    if len(head) >= 12 and head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return True
    return False


def _printable_ratio(text: str) -> float:
    if not text:
        return 0.0
    printable = 0
    for ch in text:
        code = ord(ch)
        if ch in ("\n", "\r", "\t"):
            printable += 1
            continue
        if 32 <= code <= 126:
            printable += 1
            continue
        if 0x4E00 <= code <= 0x9FFF:
            printable += 1
            continue
    return printable / max(1, len(text))


def _looks_like_text_from_head(head: bytes) -> bool:
    if not head:
        return False
    if _is_image_magic(head):
        return False

    # Fast binary rejection: too many NUL bytes usually means binary payload.
    nul_ratio = head.count(0) / max(1, len(head))
    if nul_ratio > 0.25:
        return False

    for enc in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "gbk"):
        try:
            sample = head.decode(enc)
        except Exception:
            continue
        if _printable_ratio(sample) >= 0.70:
            return True

    ascii_like = sum(1 for b in head if b in (9, 10, 13) or 32 <= b <= 126)
    if ascii_like / max(1, len(head)) >= 0.85:
        return True
    return False


def detect_file_kind(path: str | Path) -> str:
    p = Path(path)
    suffix = p.suffix.lower()
    if suffix in IMAGE_EXTS:
        return "image"
    if suffix in TEXT_EXTS:
        return "text"

    head = _read_head(p)
    if _is_image_magic(head):
        return "image"
    if _looks_like_text_from_head(head):
        return "text"
    return "unknown"


def read_text_file(path: str | Path, *, max_chars: int = 100_000) -> str:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return ""
    encodings = ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "gbk")
    for enc in encodings:
        try:
            text = p.read_text(encoding=enc)
            return text[: max(1, int(max_chars))]
        except Exception:
            continue
    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    return text[: max(1, int(max_chars))]

