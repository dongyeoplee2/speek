"""
slurm_cache.py — shared TTL cache + parallel fetch for speek-max.

All subprocess calls flow through SlurmCache.get(key, fn, ttl).
The cache is process-global (module-level singleton) so every widget
shares the same data without redundant subprocess calls.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Optional, Tuple


class SlurmCache:
    """Thread-safe TTL cache for SLURM subprocess output."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # key → (data, fetched_at_epoch)
        self._store: Dict[str, Tuple[Any, float]] = {}

    # ── public API ────────────────────────────────────────────────────────────

    def get(self, key: str, fn: Callable[[], Any], ttl: float) -> Any:
        """Return cached data if fresh, else call fn(), cache, and return."""
        with self._lock:
            entry = self._store.get(key)
            if entry is not None:
                data, fetched_at = entry
                if time.monotonic() - fetched_at < ttl:
                    return data
        # stale or missing — fetch outside the lock so other threads can proceed
        data = fn()
        with self._lock:
            self._store[key] = (data, time.monotonic())
        return data

    def get_cached(self, key: str) -> Optional[Any]:
        """Return whatever is in the cache for key (may be stale). None if missing."""
        with self._lock:
            entry = self._store.get(key)
            return entry[0] if entry else None

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def invalidate_all(self) -> None:
        with self._lock:
            self._store.clear()

    def parallel_fetch(
        self,
        tasks: list[Tuple[str, Callable[[], Any], float]],
        max_workers: int = 4,
    ) -> Dict[str, Any]:
        """
        Fetch multiple keys in parallel.
        tasks = [(key, fn, ttl), ...]
        Returns {key: data} for all tasks (only calls fn if stale).
        """
        results: Dict[str, Any] = {}
        stale = []
        with self._lock:
            for key, fn, ttl in tasks:
                entry = self._store.get(key)
                if entry and time.monotonic() - entry[1] < ttl:
                    results[key] = entry[0]
                else:
                    stale.append((key, fn))

        if not stale:
            return results

        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            futures = {exe.submit(fn): key for key, fn in stale}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    data = future.result()
                except Exception:
                    data = None
                with self._lock:
                    self._store[key] = (data, time.monotonic())
                results[key] = data

        return results


# ── module-level singleton ────────────────────────────────────────────────────
cache = SlurmCache()
