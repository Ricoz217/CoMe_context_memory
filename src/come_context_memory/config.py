from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


class ConfigNode(dict):
    """Dict with attribute-style access, recursively wrapping nested dict/list values."""

    def __init__(self, data: dict[str, Any] | None = None) -> None:
        super().__init__()
        for k, v in (data or {}).items():
            super().__setitem__(k, self._wrap(v))

    @classmethod
    def _wrap(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return cls(value)
        if isinstance(value, list):
            return [cls._wrap(x) for x in value]
        return value

    def __getattr__(self, item: str) -> Any:
        if item in self:
            return self[item]
        raise AttributeError(item)

    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = self._wrap(value)

    def copy(self) -> "ConfigNode":
        return ConfigNode(dict(self))


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


DEFAULTS: dict[str, Any] = {
    "Common": {
        "FileCacheExpire": 30,
        "MinImageQuality": 20,
        "CompressImageSizeAttempt": 5,
        "CompressImageSizeFactor": 0.7,
    },
    "LLM": {
        "ChatRequestTimeout": 300,
    },
    "llm_presets": {
        "CONTEXT_MEMORY": {
            "endpoint": os.getenv("OPENAI_API_ENDPOINT", "https://api.openai.com/v1/chat/completions"),
            "token": os.getenv("OPENAI_API_KEY", ""),
            "model": os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            "api_type": "openai",
            "max_context": 200000,
            "auto_compress_gate": 0.7,
            "extra_parameter": {},
            "proxy_mode": "",
            "price": {},
        },
        "KIMI2.6": {
            "endpoint": os.getenv("OPENAI_API_ENDPOINT", "https://api.openai.com/v1/chat/completions"),
            "token": os.getenv("OPENAI_API_KEY", ""),
            "model": os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            "api_type": "openai",
            "max_context": 200000,
            "auto_compress_gate": 0.7,
            "extra_parameter": {},
            "proxy_mode": "",
            "price": {},
        },
    },
    "proxies": {
        "LOCAL_7890": {
            "http": "http://127.0.0.1:7890",
            "https": "http://127.0.0.1:7890",
        }
    },
    "Logging": {
        "stdout_enabled": True,
        "write_error_file": True,
        "error_log_file": "logs/error.log",
    },
}


def _resolve_root_dir() -> Path:
    env_root = os.getenv("COME_CONTEXT_MEMORY_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


ROOT_DIR: Path = _resolve_root_dir()
DATA_DIR: Path = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _resolve_config_path() -> Path:
    env_cfg = os.getenv("COME_CONTEXT_MEMORY_CONFIG", "").strip()
    if env_cfg:
        return Path(env_cfg).expanduser()

    cwd = Path.cwd()
    preferred = cwd / "config" / "context_memory.yaml"
    return preferred


_CONFIG_PATH: Path = _resolve_config_path()


def _ensure_config_file(path: Path) -> None:
    if path.exists():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        legacy = path.parent / "memory.yaml"
        if legacy.exists():
            path.write_text(legacy.read_text(encoding="utf-8"), encoding="utf-8")
            return
        # Auto-generate a user-editable config file.
        text = yaml.safe_dump(DEFAULTS, allow_unicode=True, sort_keys=False)
        path.write_text(text, encoding="utf-8")
    except Exception:
        # Keep runtime tolerant: fallback to in-memory defaults when write fails.
        return



def _load_user_config() -> dict[str, Any]:
    _ensure_config_file(_CONFIG_PATH)
    if not _CONFIG_PATH.exists():
        return {}
    try:
        raw = yaml.safe_load(_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _build_setting_cfg() -> ConfigNode:
    merged = _deep_merge(DEFAULTS, _load_user_config())
    return ConfigNode(merged)


SETTING_CFG: ConfigNode = _build_setting_cfg()


def reload_config() -> ConfigNode:
    global SETTING_CFG
    SETTING_CFG = _build_setting_cfg()
    return SETTING_CFG


def get_llm(preset_name: str) -> ConfigNode:
    presets = SETTING_CFG.get("llm_presets", {})
    if not isinstance(presets, dict):
        raise TypeError("llm_presets must be a mapping")

    target = presets.get(str(preset_name), presets.get("CONTEXT_MEMORY"))
    if not isinstance(target, dict):
        raise KeyError(f"llm preset not found: {preset_name}")

    node = ConfigNode(target)
    # Backward-compatible key alias: old `auto_compress_rate` -> new `auto_compress_gate`.
    if "auto_compress_gate" not in node and "auto_compress_rate" in node:
        node["auto_compress_gate"] = node["auto_compress_rate"]
    if "auto_compress_rate" not in node and "auto_compress_gate" in node:
        node["auto_compress_rate"] = node["auto_compress_gate"]
    for key in (
        "endpoint",
        "token",
        "model",
        "api_type",
        "max_context",
        "auto_compress_gate",
        "extra_parameter",
        "proxy_mode",
        "price",
    ):
        if key not in node:
            raise KeyError(f"llm preset missing key: {key}")
    return node


def get_proxy(proxy_name_or_dict: str | dict[str, Any]) -> dict[str, Any] | str:
    if isinstance(proxy_name_or_dict, dict):
        return proxy_name_or_dict

    name = str(proxy_name_or_dict or "").strip()
    if not name:
        return ""

    if name.startswith("http://") or name.startswith("https://"):
        return {"http": name, "https": name}

    proxies = SETTING_CFG.get("proxies", {})
    if isinstance(proxies, dict) and name in proxies:
        val = proxies[name]
        if isinstance(val, str):
            return {"http": val, "https": val}
        if isinstance(val, dict):
            return val

    raise KeyError(f"proxy not found: {name}")
