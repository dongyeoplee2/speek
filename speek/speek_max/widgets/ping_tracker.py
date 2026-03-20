"""ping_tracker.py — Per-cell change-highlight tracker for table widgets.

Tracks cell-level changes within rows. When a cell value changes,
it gets a fading highlight (intensity 1.0→0.0) over a configurable duration.
Also tracks row removals as "ghosts".
"""
from __future__ import annotations

import time
from typing import Dict, Set, Tuple


# Cell key: (row_key, col_index)
CellKey = Tuple[str, int]


class PingTracker:
    """Track per-cell changes and provide fading highlight intensities.

    Usage:
        tracker = PingTracker(duration=10.0)

        # On each data refresh, call update() with per-cell signatures
        tracker.update(row_sigs)
        # row_sigs: {row_key: [cell0_sig, cell1_sig, ...]}

        # Query intensity for a specific cell
        intensity = tracker.cell_intensity(row_key, col_idx)  # 0.0–1.0

        # Get ghost rows (recently removed)
        for key, intensity in tracker.ghosts():
            ...
    """

    def __init__(self, duration: float = 10.0) -> None:
        self.duration = duration
        self._cell_pings: Dict[CellKey, float] = {}  # (row, col) → timestamp
        self._ghost_rows: Dict[str, float] = {}      # removed row_key → timestamp
        self._prev: Dict[str, list[str]] = {}         # row_key → [cell sigs]
        self._seeded: bool = False                    # first update is silent

    @property
    def has_active(self) -> bool:
        return bool(self._cell_pings) or bool(self._ghost_rows)

    def update(self, row_sigs: Dict[str, list[str]]) -> None:
        """Compare new cell signatures with previous state.

        First call is silent (seeds baseline). Subsequent calls detect changes.
        """
        if not self._seeded:
            self._prev = {k: list(v) for k, v in row_sigs.items()}
            self._seeded = True
            return

        now = time.monotonic()
        self._detect_changes(row_sigs, now)
        self._detect_removals(row_sigs, now)
        self._prev = {k: list(v) for k, v in row_sigs.items()}
        self._cleanup(now)

    def _detect_changes(self, row_sigs: Dict[str, list[str]], now: float) -> None:
        for row_key, cells in row_sigs.items():
            prev_cells = self._prev.get(row_key)
            if prev_cells is None:
                self._ping_all_cells(row_key, len(cells), now)
            else:
                self._ping_changed_cells(row_key, cells, prev_cells, now)

    def _ping_all_cells(self, row_key: str, n: int, now: float) -> None:
        for col_idx in range(n):
            self._cell_pings[(row_key, col_idx)] = now

    def _ping_changed_cells(
        self, row_key: str, cells: list[str], prev: list[str], now: float,
    ) -> None:
        for col_idx, sig in enumerate(cells):
            prev_sig = prev[col_idx] if col_idx < len(prev) else None
            if sig != prev_sig:
                self._cell_pings[(row_key, col_idx)] = now

    def _detect_removals(self, row_sigs: Dict[str, list[str]], now: float) -> None:
        for row_key in self._prev:
            if row_key not in row_sigs:
                self._ghost_rows[row_key] = now
                to_remove = [k for k in self._cell_pings if k[0] == row_key]
                for k in to_remove:
                    del self._cell_pings[k]

    def cell_intensity(self, row_key: str, col_idx: int) -> float:
        """Return 0.0–1.0 highlight intensity for a specific cell."""
        key = (row_key, col_idx)
        if key not in self._cell_pings:
            return 0.0
        elapsed = time.monotonic() - self._cell_pings[key]
        if elapsed >= self.duration:
            del self._cell_pings[key]
            return 0.0
        return 1.0 - (elapsed / self.duration)

    def row_has_ping(self, row_key: str) -> bool:
        """Return True if any cell in the row has an active ping."""
        return any(k[0] == row_key for k in self._cell_pings)

    def ghosts(self) -> list[tuple[str, float]]:
        """Return [(row_key, intensity)] for recently removed rows."""
        now = time.monotonic()
        result = []
        expired = []
        for key, ts in self._ghost_rows.items():
            elapsed = now - ts
            if elapsed >= self.duration:
                expired.append(key)
            else:
                result.append((key, 1.0 - elapsed / self.duration))
        for key in expired:
            del self._ghost_rows[key]
        return result

    def dismiss_row(self, row_key: str) -> None:
        """Remove all highlights for a row (e.g. user clicked it)."""
        to_remove = [k for k in self._cell_pings if k[0] == row_key]
        for k in to_remove:
            del self._cell_pings[k]
        self._ghost_rows.pop(row_key, None)

    def _cleanup(self, now: float) -> None:
        expired = [k for k, ts in self._cell_pings.items() if now - ts >= self.duration]
        for k in expired:
            del self._cell_pings[k]
        expired_g = [k for k, ts in self._ghost_rows.items() if now - ts >= self.duration]
        for k in expired_g:
            del self._ghost_rows[k]
