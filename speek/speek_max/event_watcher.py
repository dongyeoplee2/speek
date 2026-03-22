"""event_watcher.py — Background job state-change watcher.

Polls squeue every 30s for the current user and refreshes the Events
table when jobs transition states. When sacct is unavailable, captures
job transitions via scontrol for a limited history fallback.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Tuple

from textual.app import App

_TRANSITIONS_FILE = Path(
    os.environ.get('XDG_CACHE_HOME', Path.home() / '.cache')
) / 'speek' / 'job_transitions.json'
_MAX_TRANSITIONS = 500  # keep last N transitions


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

        # Jobs that disappeared (finished) — capture transition
        disappeared = []
        for jid, prev_state in prev_known.items():
            if jid not in current:
                if prev_state in ('RUNNING', 'PENDING'):
                    changed_jids.add(jid)
                    disappeared.append(jid)

        self._known = current

        # When sacct unavailable, capture final state via scontrol
        if disappeared and not getattr(self._app, '_cmd_sacct', True):
            self._capture_transitions(disappeared, prev_known)

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

    def _capture_transitions(self, jids: list, prev_known: dict) -> None:
        """Try scontrol show job for recently finished jobs and save to disk."""
        transitions = _load_transitions()
        now = time.time()
        for jid in jids:
            info = self._scontrol_job(jid)
            transitions.append({
                'jid': jid,
                'name': info.get('JobName', ''),
                'partition': info.get('Partition', ''),
                'state': info.get('JobState', 'UNKNOWN'),
                'elapsed': info.get('RunTime', ''),
                'start': info.get('StartTime', ''),
                'end': info.get('EndTime', ''),
                'exit_code': info.get('ExitCode', ''),
                'alloc_tres': info.get('TRES', info.get('AllocTRES', '')),
                'nodelist': info.get('NodeList', ''),
                'user': self._user,
                'ts': now,
            })
        # Prune and save
        transitions = transitions[-_MAX_TRANSITIONS:]
        _save_transitions(transitions)

    @staticmethod
    def _scontrol_job(jid: str) -> dict:
        """Get job info from scontrol (works ~5 min after completion)."""
        try:
            out = subprocess.check_output(
                ['scontrol', 'show', 'job', jid, '--oneliner'],
                text=True, stderr=subprocess.DEVNULL, timeout=5,
            )
            info = {}
            for token in out.strip().split():
                if '=' in token:
                    k, v = token.split('=', 1)
                    info[k] = v
            return info
        except Exception:
            return {}


def _load_transitions() -> list:
    """Load saved job transitions from disk."""
    try:
        return json.loads(_TRANSITIONS_FILE.read_text())
    except Exception:
        return []


def _save_transitions(data: list) -> None:
    """Save job transitions to disk."""
    try:
        _TRANSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _TRANSITIONS_FILE.write_text(json.dumps(data))
    except Exception:
        pass


def load_fallback_history(user: str = '', days: int = 7) -> List[Tuple]:
    """Load job history from transition cache (sacct fallback).

    Returns tuples matching sacct format:
    (jid, name, partition, start, elapsed, state, exit_code, '', '')
    """
    transitions = _load_transitions()
    cutoff = time.time() - days * 86400
    rows = []
    for t in reversed(transitions):
        if t.get('ts', 0) < cutoff:
            continue
        if user and t.get('user', '') != user:
            continue
        rows.append((
            t.get('jid', ''),
            t.get('name', ''),
            t.get('partition', ''),
            t.get('start', ''),
            t.get('elapsed', ''),
            t.get('state', 'UNKNOWN'),
            t.get('exit_code', ''),
            t.get('alloc_tres', ''),   # enriched from scontrol when available
            t.get('nodelist', ''),     # enriched from scontrol when available
        ))
    return rows
