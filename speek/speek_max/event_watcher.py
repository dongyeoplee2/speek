"""event_watcher.py — Background job state-change watcher.

Polls squeue every 30s for the current user and refreshes the Events
table when jobs transition states.
"""
from __future__ import annotations

import subprocess
from typing import Dict

from textual.app import App


def _parse_gpu(gres: str) -> str:
    """Extract a short GPU label from a GRES string, e.g. 'gpu:A100-80GB:4' -> 'A100-80GBx4'."""
    import re
    m = re.search(r'gpu:([A-Za-z0-9\-]+):(\d+)', gres, re.IGNORECASE)
    if m:
        return f'{m.group(1)}x{m.group(2)}'
    m = re.search(r'gpu(?::([A-Za-z0-9\-]+))?:(\d+)', gres, re.IGNORECASE)
    if m:
        return f'GPUx{m.group(2)}'
    return ''


def _query(user: str) -> Dict[str, str]:
    """Return {job_id: (state, name, gpu_label, nodes)} for all jobs belonging to user."""
    try:
        out = subprocess.check_output(
            ['squeue', '-u', user, '-o', '%i|%T|%j|%b|%N', '-h', '--states=all'],
            text=True, stderr=subprocess.DEVNULL, timeout=10,
        )
    except Exception:
        return {}
    jobs = {}
    for ln in out.splitlines():
        parts = (ln.strip().split('|') + [''] * 5)[:5]
        jid, state, name, gres, nodes = [p.strip() for p in parts]
        if jid:
            jobs[jid] = (state, name, _parse_gpu(gres), nodes)
    return jobs


class EventWatcher:
    """Attach to an App and call start() to begin watching."""

    def __init__(self, app: App, user: str, interval: float = 30.0) -> None:
        self._app = app
        self._user = user
        self._interval = interval
        self._known: Dict[str, str] = {}  # job_id -> state
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        raw = _query(self._user)
        self._known = {jid: st for jid, (st, *_) in raw.items()}
        self._app.set_interval(self._interval, self._poll)

    def _poll(self) -> None:
        self._app.run_worker(self._check, thread=True, group='event-watcher', exclusive=True)

    def _check(self) -> None:
        raw = _query(self._user)
        current = {jid: st for jid, (st, *_) in raw.items()}
        prev_known = dict(self._known)
        changed_jids: set[str] = set()

        for jid, state in current.items():
            prev = prev_known.get(jid)
            if (prev is not None and prev != state) or (prev is None and state == 'RUNNING'):
                changed_jids.add(jid)

        # Jobs that disappeared (finished)
        for jid, prev_state in prev_known.items():
            if jid not in current and prev_state == 'RUNNING':
                changed_jids.add(jid)

        self._known = current

        if changed_jids:
            def _refresh():
                try:
                    from speek.speek_max.widgets.history_widget import HistoryWidget
                    hw = self._app.query_one(HistoryWidget)
                    hw.mark_fresh(changed_jids)
                    hw._load()
                except Exception:
                    pass

            self._app.call_from_thread(_refresh)
