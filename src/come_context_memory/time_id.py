from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from come_context_memory.config import DATA_DIR


_EPOCH_MS = 1_704_067_200_000  # 2024-01-01T00:00:00Z
_SEQUENCE_BITS = 12
_SEQUENCE_MASK = (1 << _SEQUENCE_BITS) - 1
_SAVE_RETRY_COUNT = 3
_SAVE_RETRY_DELAY_SEC = 0.01
_DEFAULT_STATE_FILE = DATA_DIR / "runtime" / "time_id_state.json"

_log = logging.getLogger(__name__)


@dataclass
class _State:
    last_ms: int = 0
    sequence: int = 0


class TimeBasedIdGenerator:
    """Monotonic time-based unique ID generator with persisted state."""

    def __init__(
        self,
        state_file: str | Path | None = None,
        time_ms_fn: Callable[[], int] | None = None,
    ) -> None:
        self._state_file = Path(state_file) if state_file else _DEFAULT_STATE_FILE
        self._time_ms_fn = time_ms_fn or (lambda: time.time_ns() // 1_000_000)
        self._lock = threading.Lock()
        self._state = self._load_state()
        self._persist_degraded = False

    def next_id(self) -> int:
        with self._lock:
            now_ms = self._time_ms_fn()
            current_ms = now_ms if now_ms > self._state.last_ms else self._state.last_ms

            if current_ms == self._state.last_ms:
                next_seq = self._state.sequence + 1
                if next_seq > _SEQUENCE_MASK:
                    current_ms += 1
                    next_seq = 0
            else:
                next_seq = 0

            self._state.last_ms = current_ms
            self._state.sequence = next_seq
            new_id = ((current_ms - _EPOCH_MS) << _SEQUENCE_BITS) | next_seq
            self._save_state()
            return new_id

    def _load_state(self) -> _State:
        if not self._state_file.exists():
            return _State()
        try:
            payload = json.loads(self._state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return _State()
        return _State(
            last_ms=int(payload.get("last_ms", 0)),
            sequence=int(payload.get("sequence", 0)),
        )

    def _save_state(self) -> None:
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {"last_ms": self._state.last_ms, "sequence": self._state.sequence}
        payload_text = json.dumps(payload, ensure_ascii=True)
        last_error: OSError | None = None

        for attempt in range(1, _SAVE_RETRY_COUNT + 1):
            temp_file = self._state_file.with_suffix(
                f"{self._state_file.suffix}.{os.getpid()}.{threading.get_ident()}.{attempt}.tmp"
            )
            try:
                temp_file.write_text(payload_text, encoding="utf-8")
                os.replace(temp_file, self._state_file)
                if self._persist_degraded:
                    _log.warning("time_id state persistence recovered: %s", self._state_file)
                    self._persist_degraded = False
                return
            except OSError as exc:
                last_error = exc
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                except OSError:
                    pass
                if attempt < _SAVE_RETRY_COUNT:
                    time.sleep(_SAVE_RETRY_DELAY_SEC)

        self._persist_degraded = True
        _log.warning(
            "time_id state persistence failed after %s attempts (%s): %s",
            _SAVE_RETRY_COUNT,
            self._state_file,
            last_error,
        )


_GLOBAL_GENERATOR: TimeBasedIdGenerator | None = None
_GLOBAL_LOCK = threading.Lock()
_GLOBAL_STATE_FILE: Path = _DEFAULT_STATE_FILE


def configure_global_time_id_state_file(state_file: str | Path) -> None:
    """Update global generator persistence file path."""
    global _GLOBAL_GENERATOR, _GLOBAL_STATE_FILE
    new_file = Path(state_file)
    with _GLOBAL_LOCK:
        _GLOBAL_STATE_FILE = new_file
        # Recreate so subsequent calls use the new persistence path.
        _GLOBAL_GENERATOR = TimeBasedIdGenerator(state_file=new_file)


def get_global_time_id_generator(
    state_file: str | Path | None = None,
) -> TimeBasedIdGenerator:
    global _GLOBAL_GENERATOR, _GLOBAL_STATE_FILE
    target_file = Path(state_file) if state_file else _GLOBAL_STATE_FILE
    if _GLOBAL_GENERATOR is not None and _GLOBAL_GENERATOR._state_file == target_file:
        return _GLOBAL_GENERATOR

    with _GLOBAL_LOCK:
        if _GLOBAL_GENERATOR is None or _GLOBAL_GENERATOR._state_file != target_file:
            _GLOBAL_STATE_FILE = target_file
            _GLOBAL_GENERATOR = TimeBasedIdGenerator(state_file=target_file)
    return _GLOBAL_GENERATOR


def next_time_id() -> int:
    return get_global_time_id_generator().next_id()
