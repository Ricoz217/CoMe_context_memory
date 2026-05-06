from __future__ import annotations

import traceback
from datetime import datetime
from pathlib import Path
import threading
from typing import Any


class StatusHandle:
    def __init__(self, logger: "Logger") -> None:
        self._logger = logger

    def update(self, text: Any) -> None:
        # lightweight compatibility shim
        self._logger._emit("STATUS", str(text))


class BlockHandle:
    def __init__(self, logger: "Logger", level: str, prefix: str = "") -> None:
        self._logger = logger
        self._level = level
        self._prefix = prefix
        self._buffer: list[str] = []

    def write(self, text: Any) -> None:
        self._buffer.append(str(text))

    def __enter__(self) -> "BlockHandle":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        payload = "".join(self._buffer)
        if payload:
            self._logger._emit(self._level, f"{self._prefix}{payload}")
        return False


class Logger:
    def __init__(self, name: str = "context_memory") -> None:
        self.name = name
        self._stdout_enabled = True
        self._write_error_file = True
        self._error_log_file: Path | None = None
        self._lock = threading.Lock()
        self._refresh_from_config()

    @staticmethod
    def _fmt(msg: Any, *args: Any) -> str:
        text = str(msg)
        if args:
            try:
                return text % args
            except Exception:
                return " ".join([text, *[str(a) for a in args]])
        return text

    def _emit(self, level: str, text: str) -> None:
        self._refresh_from_config()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{level}] {ts} {text}"
        if self._stdout_enabled:
            print(line)
        if self._write_error_file and level.upper() == "ERROR":
            self._append_error_log(line)

    def _append_error_log(self, line: str) -> None:
        if self._error_log_file is None:
            return
        with self._lock:
            self._error_log_file.parent.mkdir(parents=True, exist_ok=True)
            with self._error_log_file.open("a", encoding="utf-8") as f:
                f.write(line)
                f.write("\n")

    def _refresh_from_config(self) -> None:
        try:
            from context_memory.config import ROOT_DIR, SETTING_CFG
        except Exception:
            return
        cfg = getattr(SETTING_CFG, "Logging", None)
        if cfg is None:
            return

        try:
            self._stdout_enabled = bool(getattr(cfg, "stdout_enabled", True))
        except Exception:
            self._stdout_enabled = True

        try:
            self._write_error_file = bool(getattr(cfg, "write_error_file", True))
        except Exception:
            self._write_error_file = True

        raw_path = str(getattr(cfg, "error_log_file", "logs/error.log") or "").strip()
        if not raw_path:
            self._error_log_file = None
            return
        p = Path(raw_path)
        if not p.is_absolute():
            p = ROOT_DIR / p
        self._error_log_file = p

    def status(self) -> StatusHandle:
        return StatusHandle(self)

    def debug(self, msg: Any, *args: Any, stream: bool = False) -> BlockHandle | None:
        if stream:
            return BlockHandle(self, "DEBUG", self._fmt(msg, *args))
        self._emit("DEBUG", self._fmt(msg, *args))
        return None

    def info(self, msg: Any, *args: Any, stream: bool = False) -> BlockHandle | None:
        if stream:
            return BlockHandle(self, "INFO", self._fmt(msg, *args))
        self._emit("INFO", self._fmt(msg, *args))
        return None

    def warning(self, msg: Any, *args: Any, stream: bool = False) -> BlockHandle | None:
        if stream:
            return BlockHandle(self, "WARN", self._fmt(msg, *args))
        self._emit("WARN", self._fmt(msg, *args))
        return None

    warn = warning

    def error(self, msg: Any, *args: Any, stream: bool = False) -> BlockHandle | None:
        if stream:
            return BlockHandle(self, "ERROR", self._fmt(msg, *args))
        self._emit("ERROR", self._fmt(msg, *args))
        return None

    def exception(self, msg: Any, *args: Any, stream: bool = False) -> BlockHandle | None:
        text = self._fmt(msg, *args)
        if stream:
            h = BlockHandle(self, "ERROR", text + "\n")
            h.write(traceback.format_exc())
            return h
        self._emit("ERROR", text)
        self._emit("ERROR", traceback.format_exc())
        return None


_GLOBAL_LOGGER = Logger()


def get_logger() -> Logger:
    return _GLOBAL_LOGGER
