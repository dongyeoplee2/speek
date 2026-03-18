"""event_watcher.py — Background job state-change watcher.

Polls squeue every 30s for the current user and fires app notifications
when jobs transition: PENDING→RUNNING, RUNNING→COMPLETED/FAILED/TIMEOUT/CANCELLED.
"""
from __future__ import annotations

import subprocess
from typing import Dict

from textual.app import App


_TERMINAL = {'COMPLETED', 'FAILED', 'TIMEOUT', 'CANCELLED', 'NODE_FAIL', 'OUT_OF_MEMORY'}
_SEVERITY = {
    'COMPLETED': 'information',
    'FAILED':    'error',
    'TIMEOUT':   'error',
    'CANCELLED': 'warning',
    'NODE_FAIL': 'error',
    'OUT_OF_MEMORY': 'error',
}
_EMOJI = {
    'RUNNING':   '▶',
    'COMPLETED': '✓',
    'FAILED':    '✗',
    'TIMEOUT':   '⏱',
    'CANCELLED': '⊘',
    'NODE_FAIL': '✗',
    'OUT_OF_MEMORY': '✗',
}


def _query(user: str) -> Dict[str, str]:
    """Return {job_id: state} for all jobs belonging to user."""
    try:
        out = subprocess.check_output(
            ['squeue', '-u', user, '-o', '%i|%T|%j', '-h', '--states=all'],
            text=True, stderr=subprocess.DEVNULL, timeout=10,
        )
    except Exception:
        return {}
    jobs = {}
    for ln in out.splitlines():
        parts = ln.strip().split('|')
        if len(parts) >= 2:
            jobs[parts[0].strip()] = (parts[1].strip(), parts[2].strip() if len(parts) > 2 else '')
    return jobs


class EventWatcher:
    """Attach to an App and call start() to begin watching."""

    def __init__(self, app: App, user: str, interval: float = 30.0) -> None:
        self._app = app
        self._user = user
        self._interval = interval
        self._known: Dict[str, str] = {}  # job_id → state
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        # Seed initial state silently
        raw = _query(self._user)
        self._known = {jid: st for jid, (st, _) in raw.items()}
        self._app.set_interval(self._interval, self._poll)

    def _poll(self) -> None:
        self._app.run_worker(self._check, thread=True, group='event-watcher', exclusive=True)

    def _check(self) -> None:
        raw = _query(self._user)
        current = {jid: st for jid, (st, _) in raw.items()}
        names   = {jid: nm for jid, (_, nm) in raw.items()}

        notifications = []

        for jid, state in current.items():
            prev = self._known.get(jid)
            name = names.get(jid, jid)
            if prev is None:
                # New job appeared
                if state == 'RUNNING':
                    notifications.append((f'{_EMOJI["RUNNING"]} Job started: {name} ({jid})', 'information'))
            elif prev != state:
                emoji = _EMOJI.get(state, '•')
                if prev == 'PENDING' and state == 'RUNNING':
                    notifications.append((f'{emoji} Job started: {name} ({jid})', 'information'))
                elif state in _TERMINAL:
                    sev = _SEVERITY.get(state, 'warning')
                    notifications.append((f'{emoji} Job {state.lower()}: {name} ({jid})', sev))

        # Jobs that disappeared (completed but no longer in squeue)
        for jid in list(self._known):
            if jid not in current and self._known[jid] == 'RUNNING':
                notifications.append((f'{_EMOJI["COMPLETED"]} Job finished: {jid}', 'information'))

        self._known = current

        for msg, sev in notifications:
            self._app.call_from_thread(
                lambda m=msg, s=sev: self._app.notify(m, severity=s, timeout=10)
            )
