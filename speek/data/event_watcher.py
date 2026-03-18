"""
event_watcher.py — background job-state-change detector for speek-max.

Polls squeue -u $USER every POLL_INTERVAL seconds. On state change, posts a
JobStateChanged message to the app (via call_from_thread). Also appends to
~/.cache/speek/events.jsonl for persistence.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional

from textual.message import Message

POLL_INTERVAL = 30  # seconds
EVENTS_FILE = Path.home() / ".cache" / "speek" / "events.jsonl"


# ── Messages (posted to the app) ─────────────────────────────────────────────

class JobStateChanged(Message):
    """Posted when a job transitions state."""

    def __init__(
        self,
        job_id: str,
        old_state: Optional[str],
        new_state: str,
        timestamp: float,
    ) -> None:
        super().__init__()
        self.job_id = job_id
        self.old_state = old_state
        self.new_state = new_state
        self.timestamp = timestamp

    @property
    def severity(self) -> str:
        if self.new_state in ("FAILED", "TIMEOUT", "CANCELLED"):
            return "error"
        if self.new_state == "RUNNING":
            return "information"
        if self.new_state == "COMPLETED":
            return "information"
        return "warning"

    @property
    def summary(self) -> str:
        arrow = f"{self.old_state} → " if self.old_state else ""
        return f"Job {self.job_id}: {arrow}{self.new_state}"


# ── Watcher ───────────────────────────────────────────────────────────────────

class EventWatcher:
    """
    Run in a background thread via run_worker(thread=True).
    Call start(user, post_fn) then loop() which blocks until cancelled.
    """

    def __init__(self, user: str, post_fn: Callable[[JobStateChanged], None]) -> None:
        self.user = user
        self.post_fn = post_fn
        self._prev: Dict[str, str] = {}
        self._stop_event = threading.Event()
        EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)

    def stop(self) -> None:
        self._stop_event.set()

    def loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._poll()
            except Exception:
                pass
            # interruptible sleep: wakes immediately on stop()
            self._stop_event.wait(timeout=POLL_INTERVAL)

    def _poll(self) -> None:
        try:
            out = subprocess.check_output(
                ["squeue", "-u", self.user, "-o", "%i|%T", "-h"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            return

        current: Dict[str, str] = {}
        for ln in out.splitlines():
            parts = ln.strip().split("|")
            if len(parts) < 2:
                continue
            jid, state = parts[0].strip(), parts[1].strip()
            current[jid] = state

        now = time.time()
        # detect changes
        all_ids = set(self._prev) | set(current)
        for jid in all_ids:
            old = self._prev.get(jid)
            new = current.get(jid)
            if old == new:
                continue
            if new is None:
                # job disappeared — treat as COMPLETED if it was running
                new = "COMPLETED"
            event = JobStateChanged(jid, old, new, now)
            self._log_event(event)
            self.post_fn(event)

        self._prev = current

    def _log_event(self, event: JobStateChanged) -> None:
        try:
            with open(EVENTS_FILE, "a") as f:
                f.write(
                    json.dumps(
                        {
                            "job_id": event.job_id,
                            "old_state": event.old_state,
                            "new_state": event.new_state,
                            "timestamp": event.timestamp,
                        }
                    )
                    + "\n"
                )
        except OSError:
            pass
