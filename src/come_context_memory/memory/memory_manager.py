from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class CacheEntry:
    key: str
    value: Any
    bytes_estimate: int
    last_access_ts: float
    dirty: bool = False


class MemoryManager:
    def __init__(self, *, max_bytes: int = 1_000_000_000, idle_unload_seconds: int = 24 * 3600) -> None:
        self.max_bytes = max(128 * 1024 * 1024, int(max_bytes))
        self.idle_unload_seconds = max(60, int(idle_unload_seconds))
        self._entries: dict[str, CacheEntry] = {}
        self.aggressive_mode = False
        self._aggressive_enter_ts = 0.0
        self._idle_evictions_total = 0
        self._pressure_evictions_total = 0
        self._cleanup_runs_total = 0
        self._aggressive_enters_total = 0
        self._aggressive_seconds_total = 0.0

    def touch(self, key: str) -> None:
        entry = self._entries.get(key)
        if entry is not None:
            entry.last_access_ts = time.time()

    def set(self, key: str, value: Any, *, bytes_estimate: int, dirty: bool = False) -> None:
        now = time.time()
        self._entries[key] = CacheEntry(
            key=key,
            value=value,
            bytes_estimate=max(0, int(bytes_estimate)),
            last_access_ts=now,
            dirty=bool(dirty),
        )

    def get(self, key: str) -> Any | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        entry.last_access_ts = time.time()
        return entry.value

    def mark_dirty(self, key: str, dirty: bool = True) -> None:
        entry = self._entries.get(key)
        if entry is not None:
            entry.dirty = bool(dirty)

    def remove(self, key: str) -> CacheEntry | None:
        return self._entries.pop(key, None)

    def total_bytes(self) -> int:
        return sum(e.bytes_estimate for e in self._entries.values())

    def cleanup(self, *, force_aggressive: bool = False) -> list[CacheEntry]:
        now = time.time()
        self._cleanup_runs_total += 1
        evicted: list[CacheEntry] = []

        idle_keys = [
            k
            for k, e in self._entries.items()
            if (now - e.last_access_ts) >= self.idle_unload_seconds
        ]
        for k in idle_keys:
            item = self._entries.pop(k, None)
            if item is not None:
                evicted.append(item)
                self._idle_evictions_total += 1

        total = self.total_bytes()
        next_aggressive = force_aggressive or total > self.max_bytes
        if next_aggressive and not self.aggressive_mode:
            self._aggressive_enters_total += 1
            self._aggressive_enter_ts = now
        elif (not next_aggressive) and self.aggressive_mode and self._aggressive_enter_ts > 0:
            self._aggressive_seconds_total += max(0.0, now - self._aggressive_enter_ts)
            self._aggressive_enter_ts = 0.0
        self.aggressive_mode = next_aggressive
        if not self.aggressive_mode:
            return evicted

        ordered = sorted(self._entries.values(), key=lambda e: e.last_access_ts)
        for entry in ordered:
            if self.total_bytes() <= self.max_bytes:
                break
            popped = self._entries.pop(entry.key, None)
            if popped is not None:
                evicted.append(popped)
                self._pressure_evictions_total += 1

        return evicted

    def diagnostics(self) -> dict[str, float | int | bool]:
        now = time.time()
        seconds_total = float(self._aggressive_seconds_total)
        if self.aggressive_mode and self._aggressive_enter_ts > 0:
            seconds_total += max(0.0, now - self._aggressive_enter_ts)
        return {
            "aggressive_mode": bool(self.aggressive_mode),
            "idle_evictions_total": int(self._idle_evictions_total),
            "pressure_evictions_total": int(self._pressure_evictions_total),
            "cleanup_runs_total": int(self._cleanup_runs_total),
            "aggressive_enters_total": int(self._aggressive_enters_total),
            "aggressive_seconds_total": float(seconds_total),
        }
