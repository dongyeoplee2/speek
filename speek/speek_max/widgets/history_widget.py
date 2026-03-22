"""history_widget.py — sacct job history panel."""
from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.reactive import reactive
from speek.speek_max.widgets.modal_base import SpeekModal
from textual.widget import Widget
from textual.widgets import (
    Button, Label, LoadingIndicator, Static,
    TabbedContent, TabPane,
)

from speek.speek_max.slurm import fetch_history, fetch_active_jobs_scontrol, SacctUnavailable
from speek.speek_max._utils import tc, safe, state_sym, state_badge, STATE_SYMBOL
from speek.speek_max.widgets.datatable import SpeekDataTable
from speek.speek_max.widgets.foldable_table import (
    FoldableTableMixin, FoldGroup, FoldMode, Leaf, Divider, Spacer,
    TreeNode, TableContext, _build_divider_cells,
)

# ── Persistence ────────────────────────────────────────────────────────────────

_CACHE_FILE = Path.home() / '.cache' / 'speek' / 'history_read.json'


def _load_cache() -> tuple[set[str], float]:
    """Load read IDs and last activation timestamp."""
    try:
        data = json.loads(_CACHE_FILE.read_text())
        if isinstance(data, list):
            return set(data), 0.0
        return set(data.get('read_ids', [])), float(data.get('last_activation', 0))
    except Exception:
        return set(), 0.0


def _save_cache(ids: set[str], last_activation: float = 0.0) -> None:
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps({
            'read_ids': list(ids),
            'last_activation': last_activation,
        }))
    except Exception:
        pass


def _load_read_ids() -> set[str]:
    return _load_cache()[0]


def _save_read_ids(ids: set[str], max_ids: int = 2000) -> None:
    _, last_act = _load_cache()
    # Trim: keep only the most recent IDs (highest job numbers)
    if len(ids) > max_ids:
        try:
            sorted_ids = sorted(ids, key=lambda x: int(x) if x.isdigit() else 0, reverse=True)
            ids = set(sorted_ids[:max_ids])
        except Exception:
            pass
    _save_cache(ids, last_act)


# ── Aggregation helpers ────────────────────────────────────────────────────────

_TRAIL_RE = re.compile(r'[\d_\-\.]+$')


def _name_base(name: str) -> str:
    return _TRAIL_RE.sub('', name)


def _should_merge(a: str, b: str) -> bool:
    if a == b:
        return True
    ba, bb = _name_base(a), _name_base(b)
    return bool(ba and ba == bb)


def _time_bucket(start_str: str, bucket_min: int = 30) -> int:
    """30-minute bucket index for grouping temporally close jobs."""
    try:
        dt = datetime.strptime(
            start_str.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S'
        )
        return int(dt.timestamp()) // (bucket_min * 60)
    except Exception:
        return 0


_GPU_RE = re.compile(r'gres/gpu(?::([a-z0-9_-]+))?(?::(\d+)|=(\d+))', re.IGNORECASE)

# ── Time-zone dividers ─────────────────────────────────────────────────────────

_DIV_KEY_PREFIX = '_div_'
_IND_KEY_PREFIX = 'ind::'
_HISTORY_TC = '#history-tc'

# (upper age bound in seconds, display label)
def _time_zone_parts(zone_idx: int) -> tuple:
    """Return (age_label, date_str) for a divider row."""
    from datetime import timedelta
    now = datetime.now()
    bounds = [3600, 86400, 3*86400, 7*86400, float('inf')]
    labels = ['1h', '1d', '3d', '7d', '7d+']
    age = labels[min(zone_idx, len(labels)-1)]
    if zone_idx >= 1:
        secs = bounds[zone_idx]
        dt = now - timedelta(seconds=secs) if secs != float('inf') else now - timedelta(seconds=bounds[zone_idx - 1])
        return age, dt.strftime('%y-%m-%d')
    return age, now.strftime('%H:%M')

_TIME_ZONE_BOUNDS = [
    (3600,         '< 1h'),
    (86400,        '1h – 24h'),
    (3 * 86400,    '1d – 3d'),
    (7 * 86400,    '3d – 7d'),
    (float('inf'), '> 7d'),
]


def _time_zone_idx(start_str: str) -> int:
    """Return time-zone index (0=<1h … 4=older)."""
    try:
        dt = datetime.strptime(
            start_str.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S'
        )
        age = (datetime.now() - dt).total_seconds()
        for i, (bound, _) in enumerate(_TIME_ZONE_BOUNDS):
            if age < bound:
                return i
    except Exception:
        pass
    return len(_TIME_ZONE_BOUNDS) - 1


_HISTORY_COL_WIDTHS = [12, 3, 3, 2, 6, 5, 7, 7, 8]

_N_HISTORY_COLS = 9  # Name + E + Ago + #J + #G + Part + Nodes + Elapsed + IDs


def _parse_gpu(alloc_tres: str) -> str:
    """Return gpu model name from AllocTRES string (e.g. 'gres/gpu:a100=2' -> 'a100')."""
    m = _GPU_RE.search(alloc_tres)
    if m and m.group(1):
        return m.group(1).lower()
    return ''


def _parse_gpu_count(alloc_tres: str) -> int:
    """Return gpu count from AllocTRES string (e.g. 'gres/gpu:a100=2' -> 2)."""
    m = _GPU_RE.search(alloc_tres)
    if m:
        return int(m.group(2) or m.group(3) or 1)
    return 0


def _fmt_gpu(models: set) -> str:
    return ','.join(sorted(m for m in models if m)) or ''


def _fmt_nodes(nodes: set) -> str:
    return ','.join(sorted(n for n in nodes if n and n != 'None')) or ''


# Maps state -> (letter, bg_tv_key, bg_fallback)
# Badge renders as: dark text ON colored background
_TYPE_BADGE: Dict[str, Tuple[str, str, str]] = {
    'COMPLETED':     ('✔', '_cyan',    '#4A9FD9'),
    'FAILED':        ('✗', 'error',    'red'),
    'TIMEOUT':       ('⏱', 'warning',  'yellow'),
    'CANCELLED':     ('⊘', 'text-muted', 'bright_black'),
    'OUT_OF_MEMORY': ('☢', 'error',    'red'),
    'PENDING':       ('⏸', 'warning',  'yellow'),
    'RUNNING':       ('▶', 'success',  'green'),
    'NODE_FAIL':     ('⚡', 'error',    'red'),
    'PREEMPTED':     ('⏏', 'warning',  'yellow'),
    'SUSPENDED':     ('⏯', 'warning',  'yellow'),
    'REQUEUED':      ('↻', 'warning',  'yellow'),
}

_COL_WIDTHS: Dict[str, int] = {
    'ago': 3, 'name': 12, 'gpu': 6, 'nodes': 5, 'count': 2, 'state': 3, 'elapsed': 7, 'ids': 8,
}


def _type_badge(state: str, tv: dict) -> Text:
    """Return a symbol badge with colored background for the given job state."""
    from speek.speek_max._utils import state_badge
    return state_badge(state)


def _rel_time(start_str: str, mode: str = 'relative') -> str:
    """Format timestamp using the shared formatter."""
    from speek.speek_max._utils import fmt_time
    result = fmt_time(start_str, mode)
    if result:
        return result
    try:
        dt = datetime.strptime(
            start_str.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S'
        )
        s = (datetime.now() - dt).total_seconds()
        if s < 3600:
            return f'{max(1, int(s / 60))}m'
        if s < 86400:
            return f'{int(s / 3600)}h'
        return f'{int(s / 86400)}d'
    except Exception:
        return start_str[:10] if start_str else ''




def _fmt_ids(ids: List[str], max_len: int = 22) -> str:
    """Compact range notation: 1234-1238,1241+2..."""
    try:
        ints = sorted(int(i) for i in ids)
    except ValueError:
        return ', '.join(ids[:3])
    groups, start, prev = [], ints[0], ints[0]
    for x in ints[1:]:
        if x == prev + 1:
            prev = x
        else:
            groups.append((start, prev))
            start = prev = x
    groups.append((start, prev))
    parts = [str(a) if a == b else f'{a}-{b}' for a, b in groups]
    result = ''
    for i, p in enumerate(parts):
        buf = (result + ',' + p) if result else p
        if len(buf) > max_len:
            result += f'+{len(parts) - i}...'
            break
        result = buf
    return result


def _find_matching_group(grouped: List[dict], part: str, state: str, tb: int, name: str) -> Optional[dict]:
    """Return the first group that matches partition/state/time-bucket and name similarity."""
    for g in grouped:
        if (g['part'] == part and g['state'] == state
                and abs(g['tb'] - tb) <= 1
                and _should_merge(g['disp_name'], name)):
            return g
    return None


def _new_group(jid: str, name: str, part: str, start: str, elapsed: str,
               state: str, tb: int, gpu_model: str, nodelist: str,
               gpu_count: int = 0) -> dict:
    return {
        'first_jid': jid, 'disp_name': name, 'part': part, 'state': state,
        'start': start, 'elapsed': elapsed, 'tb': tb, 'ids': [jid],
        'gpu_models': {gpu_model} if gpu_model else set(),
        'nodes': {nodelist} if nodelist and nodelist != 'None' else set(),
        'gpu_count': gpu_count,
    }


def _merge_into(g: dict, jid: str, start: str, elapsed: str,
                gpu_model: str, nodelist: str, gpu_count: int = 0) -> None:
    g['ids'].append(jid)
    g['gpu_count'] = g.get('gpu_count', 0) + gpu_count
    if elapsed > g['elapsed']:
        g['elapsed'] = elapsed
    if start < g['start']:
        g['start'] = start
    if gpu_model:
        g['gpu_models'].add(gpu_model)
    if nodelist and nodelist != 'None':
        g['nodes'].add(nodelist)


def _aggregate(rows: List[Tuple]) -> Tuple[List[dict], Dict[str, List[str]]]:
    """Group rows by name similarity within 30-min temporal windows.

    Returns:
        groups: list of group dicts (newest first)
        first_to_all: map from first_jid to all jids in that group
    """
    grouped: List[dict] = []
    for row in rows:
        jid, name, part, start, elapsed, state, _, alloc_tres, nodelist = row
        tb = _time_bucket(start)
        gpu_model = _parse_gpu(alloc_tres)
        gpu_count = _parse_gpu_count(alloc_tres)
        matched = _find_matching_group(grouped, part, state, tb, name)
        if matched is None:
            grouped.append(_new_group(jid, name, part, start, elapsed,
                                      state, tb, gpu_model, nodelist, gpu_count))
        else:
            _merge_into(matched, jid, start, elapsed, gpu_model, nodelist, gpu_count)

    first_to_all: Dict[str, List[str]] = {
        g['first_jid']: g['ids'] for g in grouped
    }
    return grouped, first_to_all


# ── Widget ─────────────────────────────────────────────────────────────────────

_DT_IDS = {'unread': 'dt-unread', 'read': 'dt-read', 'all': 'dt-all'}


_RECENT_DAYS = 6


def _is_recent(g: dict) -> bool:
    """True if the group's start is within the last _RECENT_DAYS days."""
    try:
        dt = datetime.strptime(
            g['start'].replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S'
        )
        return (datetime.now() - dt).total_seconds() < _RECENT_DAYS * 86400
    except Exception:
        return True


class FullHistoryModal(SpeekModal):
    """Popup showing all history regardless of age."""

    BINDINGS = [Binding('escape', 'dismiss', 'Close', show=True)]

    def __init__(self, groups: List[dict], build_row, state_style_fn) -> None:
        super().__init__()
        self._groups       = groups
        self._build_row    = build_row
        self._state_style_fn = state_style_fn

    def compose(self) -> ComposeResult:
        """Compose the full history modal."""
        with Static(classes='modal-body speek-popup', id='fh-body'):
            yield SpeekDataTable(id='fh-dt', cursor_type='row', show_cursor=True)

    def on_mount(self) -> None:
        dt = self.query_one('#fh-dt', SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('Name',    width=22)
        dt.add_column('E',       width=3)
        dt.add_column('Ago',     width=3)
        dt.add_column('#J',       width=2)
        dt.add_column('#G',      width=3)
        dt.add_column('Part',     width=6)
        dt.add_column('Nodes',   width=8)
        dt.add_column('Elapsed', width=7)
        dt.add_column('IDs',     width=14)
        tv = self.app.theme_variables
        c_muted     = tc(tv, 'text-muted',     'bright_black')
        c_secondary = tc(tv, 'text-secondary', 'default')

        state_style = self._state_style_fn(tv)
        current_zone = -1
        div_counter = 0
        for g in self._groups:
            zone = _time_zone_idx(g['start'])
            if zone != current_zone:
                if current_zone >= 0:  # spacer before divider (skip first)
                    dt.add_row(*[Text('') for _ in range(_N_HISTORY_COLS)], key=f'{_DIV_KEY_PREFIX}{div_counter}_sp')
                current_zone = zone
                _age, _date = _time_zone_parts(zone)
                cells = [Text(' ') for _ in range(_N_HISTORY_COLS)]
                _label = f'| {_age} {_date}'
                cells[0] = Text(_label.ljust(22), style='bold black on white')
                dt.add_row(*cells, key=f'{_DIV_KEY_PREFIX}{div_counter}')
                div_counter += 1
            time_mode = getattr(self.app, '_time_format', 'relative')
            row = self._build_row(g, state_style, c_muted, c_secondary, tv, time_mode=time_mode)
            dt.add_row(*row, key=g['first_jid'])


class HistoryWidget(FoldableTableMixin, Widget):
    """sacct job history grouped by name + temporal affinity."""

    can_focus = True

    BINDINGS = [
        Binding('d',     'view_log',     'Details',  show=True),
        Binding('enter', 'view_log',    'Details',  show=False),
        Binding('l',     'view_log',    'Details',  show=False),
        Binding('R',     'relaunch',    'Relaunch', show=True),
        Binding('1',     'tab_unread',  '',         show=False),
        Binding('2',     'tab_read',    '',         show=False),
        Binding('3',     'tab_all',     '',         show=False),
        Binding('v',     'expand_group', '▶▼',      show=True),
        Binding('V',     'fold_all',    '',         show=False),
        Binding('S',     'toggle_all_read', 'Read all', show=True),
        Binding('space', 'toggle_read', '☐/☑',     show=False),
        Binding('A',     'mark_all',    '',         show=False),
        Binding('a',     'full_history','',         show=False),
        Binding('r',     'refresh',     'Refresh',  show=True),
    ]

    lookback_days: reactive[int] = reactive(7)

    _FRESH_DURATION = 600.0  # 10 minutes fade
    _BLINK_DURATION = 10.0   # 10 second blink for any change

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        read_ids, self._last_activation = _load_cache()
        self._read_ids: set[str]               = read_ids
        self._all_rows: List[Tuple]            = []
        self._all_groups: List[dict]           = []
        self._first_to_all: Dict[str, List[str]] = {}
        self._jid_to_row: Dict[str, Tuple]     = {}
        self._oom_jobs: set[str]              = set()  # jids with OOM in logs
        self._oom_notified: set[str]          = set()
        # Load cached OOM verdicts immediately for instant badges on startup
        self._load_oom_disk()
        for jid, is_oom in self._oom_scan_cache.items():
            if is_oom:
                self._oom_jobs.add(jid)
        # fresh_jids: job_id -> monotonic timestamp when it became fresh
        self._fresh_jids: Dict[str, float]     = {}
        self._startup_wall: float              = 0.0  # wall-clock at startup

    def mark_fresh(self, jids: set[str]) -> None:
        """Mark job IDs as fresh (newly changed). Called by EventWatcher."""
        import time
        now = time.monotonic()
        for jid in jids:
            self._fresh_jids[jid] = now

    def dismiss_fresh(self, jid: str) -> None:
        """Remove fresh highlight (e.g. when details are opened)."""
        self._fresh_jids.pop(jid, None)

    def _fresh_intensity(self, jid: str) -> float:
        """Return 0.0-1.0 highlight intensity for a fresh job. 0 = expired."""
        if jid not in self._fresh_jids:
            return 0.0
        import time
        elapsed = time.monotonic() - self._fresh_jids[jid]
        if elapsed >= self._FRESH_DURATION:
            del self._fresh_jids[jid]
            return 0.0
        return 1.0 - (elapsed / self._FRESH_DURATION)

    def _is_blinking(self, jid: str) -> bool:
        """Return True if the job is in the initial blink phase (<10s)."""
        if jid not in self._fresh_jids:
            return False
        import time
        elapsed = time.monotonic() - self._fresh_jids[jid]
        return elapsed < self._BLINK_DURATION

    def _mark_since_last_activation(self) -> None:
        """On startup, mark events whose start time is after the last activation as fresh."""
        if not self._last_activation or not self._all_groups:
            return
        import time
        now_mono = time.monotonic()
        now_wall = time.time()
        for g in self._all_groups:
            try:
                dt = datetime.strptime(
                    g['start'].replace('T', ' ').split('.')[0],
                    '%Y-%m-%d %H:%M:%S',
                )
                event_wall = dt.timestamp()
            except Exception:
                continue
            if event_wall > self._last_activation:
                # How long ago (in monotonic time) this event happened
                age = now_wall - event_wall
                if age < self._FRESH_DURATION:
                    # Set the fresh timestamp so the remaining fade is correct
                    self._fresh_jids[g['first_jid']] = now_mono - age
        if self._fresh_jids:
            self._refresh_all_tables()

    def _tick_fresh(self) -> None:
        """Periodically re-render tables while fresh highlights are fading."""
        if self._fresh_jids:
            self._refresh_all_tables()

    class UnreadCount(Message):
        def __init__(self, count: int) -> None:
            super().__init__()
            self.count = count

    class StatusCounts(Message):
        """Per-state unread event counts for the app header."""
        def __init__(self, failed: int, timeout: int, completed: int) -> None:
            super().__init__()
            self.failed    = failed
            self.timeout   = timeout
            self.completed = completed

    class OomCount(Message):
        def __init__(self, count: int) -> None:
            super().__init__()
            self.count = count

    def compose(self) -> ComposeResult:
        """Compose the history widget."""
        with Vertical(id='history-outer'):
            with Horizontal(id='history-toolbar'):
                yield Label('Scope:', id='lb-label')
                yield Static('7d', id='lb-scope')
                yield Static('', id='toolbar-spacer')
                yield Button('☑ All Read',   id='mark-all-read-btn',   variant='default')
                yield Button('☐ All Unread', id='mark-all-unread-btn', variant='default')
                yield Button('↺', id='collect-btn', variant='default')
            yield LoadingIndicator()
            with TabbedContent(id='history-tc', initial='tc-unread'):
                with TabPane('1 Unread', id='tc-unread'):
                    yield Static('no unread events', id='empty-unread', classes='empty-state')
                    yield SpeekDataTable(id='dt-unread', cursor_type='row', show_cursor=True)
                    yield Static('', id='stats-unread', classes='history-stats')
                with TabPane('2 Read', id='tc-read'):
                    yield Static('no read events', id='empty-read', classes='empty-state')
                    yield SpeekDataTable(id='dt-read', cursor_type='row', show_cursor=True)
                    yield Static('', id='stats-read', classes='history-stats')
                with TabPane('3 All', id='tc-all'):
                    yield Static('', id='empty-all', classes='empty-state')
                    yield SpeekDataTable(id='dt-all', cursor_type='row', show_cursor=True)
                    yield Static('', id='stats-all', classes='history-stats')

    def on_mount(self) -> None:
        import time as _time
        self.border_title = 'Events'
        self._startup_wall = _time.time()
        # All tabs share one ctx for expanded state
        self._ctx = self._init_ctx(renderer=self._render_cell, n_cols=_N_HISTORY_COLS, name_col_width=22)
        self._trees: Dict[str, List[TreeNode]] = {'unread': [], 'read': [], 'all': []}
        for dt_id in _DT_IDS.values():
            self._setup_dt(self.query_one(f'#{dt_id}', SpeekDataTable))
        # Sync with app-level setting (set in Config tab)
        self.lookback_days = getattr(self.app, '_history_lookback_days', 7)
        self._update_scope_label()
        if getattr(self.app, '_feat_history', True):
            self._load()
            # After first load, mark events that changed since last activation
            self._mark_since_last_activation()
        interval = getattr(self.app, '_history_refresh', 30)
        self._refresh_timer = self.set_interval(interval, self._load)
        # Timer to update fading highlights (every 10s while fresh items exist)
        self.set_interval(10, self._tick_fresh)
        # Save current activation time for next launch
        _save_cache(self._read_ids, self._startup_wall)

    def set_refresh_interval(self, seconds: int) -> None:
        try:
            self._refresh_timer.stop()
        except Exception:
            pass
        self._refresh_timer = self.set_interval(seconds, self._load)

    def _setup_dt(self, dt: SpeekDataTable) -> None:
        dt.zebra_stripes = True
        dt.add_column('Name',    width=22)
        dt.add_column('E',       width=3)
        dt.add_column('Ago',     width=3)
        dt.add_column('#J',       width=2)
        dt.add_column('#G',      width=3)
        dt.add_column('Part',     width=6)
        dt.add_column('Nodes',   width=8)
        dt.add_column('Elapsed', width=7)
        dt.add_column('IDs',     width=14)

    def watch_lookback_days(self, _old: int, _new: int) -> None:
        self._update_scope_label()
        self._load()

    def _update_scope_label(self) -> None:
        d = self.lookback_days
        label = f'{d}d' if d < 365 else 'all'
        try:
            self.query_one('#lb-scope', Static).update(label)
        except Exception:
            pass

    def set_lookback(self, days: int) -> None:
        """Called by Config widget when the lookback setting changes."""
        self.lookback_days = days

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'collect-btn':
            self._load()
        elif event.button.id == 'mark-all-read-btn':
            self._mark_all_read()
        elif event.button.id == 'mark-all-unread-btn':
            self._mark_all_unread()

    def on_click(self, event) -> None:
        try:
            self._active_dt().focus()
        except Exception:
            pass

    def _load(self) -> None:
        if getattr(self.app, '_cmd_sacct', True) and getattr(self.app, '_feat_history', True):
            # Level 1: sacct (full history, any time range)
            self.border_subtitle = ''
            self.query_one(LoadingIndicator).display = True
            days = self.lookback_days

            def _sacct_with_fallback():
                try:
                    return fetch_history(days)
                except SacctUnavailable:
                    # slurmdbd is down — disable sacct and fall to scontrol+cache
                    self.app._cmd_sacct = False
                    self.app.notify(
                        'sacct unavailable (slurmdbd down) — using scontrol fallback',
                        severity='warning', timeout=6,
                    )
                    return self._scontrol_fallback(days)

            self.run_worker(
                _sacct_with_fallback,
                thread=True, exclusive=True, group='history',
            )
        elif getattr(self.app, '_cmd_scontrol', True):
            # Level 2: scontrol show job (active/recent) + transition cache
            self.border_subtitle = '[dim]scontrol — active + cached history[/dim]'
            self.query_one(LoadingIndicator).display = True
            days = self.lookback_days
            self.run_worker(
                lambda: self._scontrol_fallback(days),
                thread=True, exclusive=True, group='history',
            )
        else:
            # Level 3: transition cache only
            from speek.speek_max.event_watcher import load_fallback_history
            days = self.lookback_days
            self.border_subtitle = '[dim]cache only — limited history[/dim]'
            self.run_worker(
                lambda: load_fallback_history(
                    user=getattr(self.app, 'user', ''), days=days),
                thread=True, exclusive=True, group='history',
            )

    def _scontrol_fallback(self, days: int) -> List[Tuple]:
        """Merge scontrol active jobs + event watcher transition cache."""
        from speek.speek_max.event_watcher import load_fallback_history
        user = getattr(self.app, 'user', '')
        active = fetch_active_jobs_scontrol()
        cached = load_fallback_history(user=user, days=days)
        seen: set = set()
        merged: List[Tuple] = []
        for row in active:
            if row[0] not in seen:
                seen.add(row[0])
                merged.append(row)
        for row in cached:
            if row[0] not in seen:
                seen.add(row[0])
                merged.append(row)
        return merged

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.worker.group == 'history' and event.state == WorkerState.SUCCESS:
            rows = event.worker.result
            self._all_rows = list(reversed(rows))
            self._jid_to_row = {row[0]: row for row in rows}
            self._all_groups, self._first_to_all = _aggregate(self._all_rows)
            self._ctx.expanded &= set(self._first_to_all.keys())
            self.query_one(LoadingIndicator).display = False
            self._refresh_all_tables()
            # Scan for OOM in completed/running jobs
            self._scan_oom()
        if event.worker.group == 'oom-scan' and event.state == WorkerState.SUCCESS:
            new_oom = (event.worker.result or set()) - self._oom_jobs
            self._oom_jobs |= (event.worker.result or set())
            if new_oom:
                # Mark OOM groups as unread + fresh so they surface as new events
                import time as _t
                now_mono = _t.monotonic()
                for g in self._all_groups:
                    if any(jid in new_oom for jid in g['ids']):
                        # Remove from read so it appears in Unread tab
                        for jid in g['ids']:
                            self._read_ids.discard(jid)
                        # Mark fresh so it blinks
                        self._fresh_jids[g['first_jid']] = now_mono
                self.post_message(self.OomCount(len(self._oom_jobs)))
                self._refresh_all_tables()

    _oom_scan_cache: Dict[str, bool] = {}  # in-memory session cache
    _OOM_CACHE_FILE = Path(
        os.environ.get('XDG_CACHE_HOME', Path.home() / '.cache')
    ) / 'speek' / 'oom_verdicts.json'
    _oom_disk_loaded = False

    @classmethod
    def _load_oom_disk(cls) -> None:
        """Load persistent OOM verdicts from disk (once per session)."""
        if cls._oom_disk_loaded:
            return
        cls._oom_disk_loaded = True
        try:
            import json
            data = json.loads(cls._OOM_CACHE_FILE.read_text())
            if isinstance(data, dict):
                cls._oom_scan_cache.update(data)
        except Exception:
            pass

    @classmethod
    def _save_oom_disk(cls) -> None:
        """Persist OOM verdicts for completed jobs to disk.

        Prunes to 5000 entries max (keeping most recent job IDs).
        """
        try:
            import json
            data = cls._oom_scan_cache
            if len(data) > 5000:
                # Keep most recent 5000 (highest job IDs)
                sorted_keys = sorted(data.keys(), key=lambda k: int(k) if k.isdigit() else 0)
                data = {k: data[k] for k in sorted_keys[-5000:]}
                cls._oom_scan_cache = data
            cls._OOM_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            cls._OOM_CACHE_FILE.write_text(json.dumps(data))
        except Exception:
            pass

    def _scan_oom(self) -> None:
        """Scan job logs for OOM. Results cached to disk for instant startup.

        COMPLETED jobs: cached permanently on disk — never re-scanned.
        RUNNING jobs: re-scanned each cycle (logs grow).
        """
        # Load disk cache on first call
        self._load_oom_disk()

        # Apply cached verdicts immediately
        for jid, is_oom in self._oom_scan_cache.items():
            if is_oom and jid not in self._oom_jobs:
                self._oom_jobs.add(jid)

        candidates = []
        for g in self._all_groups:
            state = g['state'].split()[0] if g['state'] else ''
            if state not in ('COMPLETED', 'RUNNING'):
                continue
            for jid in g['ids']:
                if jid in self._oom_jobs:
                    continue
                if state == 'COMPLETED' and jid in self._oom_scan_cache:
                    continue
                candidates.append((jid, state))

        if not candidates:
            return

        def _worker():
            from speek.speek_max.slurm import get_job_log_path
            from speek.speek_max.log_scan import detect_oom
            oom_found: set[str] = set()
            for jid, _state in candidates:
                path = get_job_log_path(jid)
                is_oom = bool(path and detect_oom(path))
                self._oom_scan_cache[jid] = is_oom
                if is_oom:
                    oom_found.add(jid)
            # Persist to disk after scan
            self._save_oom_disk()
            return oom_found

        self.run_worker(_worker, thread=True, group='oom-scan')

    def _active_dt(self) -> SpeekDataTable:
        tc_widget = self.query_one(_HISTORY_TC, TabbedContent)
        dt_id = _DT_IDS.get(tc_widget.active.removeprefix('tc-'), 'dt-unread')
        return self.query_one(f'#{dt_id}', SpeekDataTable)

    def _active_tab(self) -> str:
        tc_widget = self.query_one(_HISTORY_TC, TabbedContent)
        return tc_widget.active.removeprefix('tc-')

    def _group_is_unread(self, g: dict) -> bool:
        return any(jid not in self._read_ids for jid in g['ids'])

    _EVENT_EXCLUDE = {'PENDING', 'RUNNING'}

    def _filtered_groups(self, tab: str) -> List[dict]:
        recent = [g for g in self._all_groups
                  if _is_recent(g) and g.get('state') not in self._EVENT_EXCLUDE]
        if tab == 'unread':
            return [g for g in recent if self._group_is_unread(g)]
        if tab == 'read':
            return [g for g in recent if not self._group_is_unread(g)]
        return recent

    def _state_style_dict(self, tv: dict) -> dict:
        c_muted   = tc(tv, 'text-muted',   'bright_black')
        c_error   = tc(tv, 'text-error',   'red')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        return {
            'COMPLETED':     'bold #4A9FD9',
            'FAILED':        f'bold {c_error}',
            'TIMEOUT':       f'bold {c_error}',
            'CANCELLED':     f'bold {c_muted}',
            'OUT_OF_MEMORY': f'bold {c_error}',
            'RUNNING':       f'bold {c_success}',
            'PENDING':       f'bold {c_warning}',
        }

    # ── Tree building ────────────────────────────────────────────────────────

    def _build_tree(self, groups: List[dict]) -> List[TreeNode]:
        """Build tree: Divider (time zones) -> event groups (FoldGroup if count > 1, else Leaf)
        -> individual events (Leaf)."""
        tree: List[TreeNode] = []
        current_zone = -1
        div_counter = 0
        for g in groups:
            zone = _time_zone_idx(g['start'])
            if zone != current_zone:
                if current_zone >= 0:
                    tree.append(Spacer(key=f'{_DIV_KEY_PREFIX}{div_counter}_sp'))
                current_zone = zone
                _age, _date = _time_zone_parts(zone)
                tree.append(Divider(
                    key=f'{_DIV_KEY_PREFIX}{div_counter}',
                    label=f'{_age} {_date}',
                ))
                div_counter += 1

            n = len(g['ids'])
            if n > 1:
                # Expandable group
                ind_children = [
                    Leaf(
                        key=f'{_IND_KEY_PREFIX}{jid}',
                        data={'jid': jid, 'group': g},
                        indent=5,
                    )
                    for jid in g['ids']
                ]
                tree.append(FoldGroup(
                    key=g['first_jid'],
                    fold_key=g['first_jid'],
                    data={'group': g},
                    children=ind_children,
                    mode=FoldMode.EXPANDED_SET,
                    indent=3,
                ))
            else:
                tree.append(Leaf(
                    key=g['first_jid'],
                    data={'group': g},
                    indent=3,
                ))
        return tree

    def _render_cell(self, node: TreeNode, is_collapsed: bool, n_cols: int) -> List[Text]:
        """Render a single tree node for the Events tables."""
        tv = self.app.theme_variables
        c_muted     = tc(tv, 'text-muted',     'bright_black')
        c_secondary = tc(tv, 'text-secondary', 'default')
        state_style = self._state_style_dict(tv)
        time_mode = getattr(self.app, '_time_format', 'relative')

        if isinstance(node, FoldGroup):
            g = node.data['group']
            return list(self._build_row(g, state_style, c_muted, c_secondary, tv,
                                        expanded=not is_collapsed, time_mode=time_mode))

        if isinstance(node, Leaf):
            d = node.data
            if 'group' in d and 'jid' not in d:
                # Top-level single group leaf
                g = d['group']
                return list(self._build_row(g, state_style, c_muted, c_secondary, tv,
                                            time_mode=time_mode))
            elif 'jid' in d:
                # Individual job under expanded group
                jid = d['jid']
                row_data = self._jid_to_row.get(jid)
                if row_data:
                    return list(self._build_individual_row(
                        row_data, state_style, c_muted, c_secondary, tv, time_mode,
                        is_last=node.is_last))
                return [Text('') for _ in range(n_cols)]

        return [Text('') for _ in range(n_cols)]

    @safe('Events refresh')
    def _refresh_all_tables(self) -> None:
        for tab, dt_id in _DT_IDS.items():
            dt    = self.query_one(f'#{dt_id}', SpeekDataTable)
            stats = self.query_one(f'#stats-{tab}', Static)
            groups = self._filtered_groups(tab)
            self._trees[tab] = self._build_tree(groups)
            self._rebuild(dt, self._ctx, self._trees[tab])
            stats.update(self._stats_text(groups, tab))
            empty = len(groups) == 0
            dt.display = not empty
            stats.set_class(empty, 'history-empty')
            # Show/hide empty-state placeholder
            try:
                es = self.query_one(f'#empty-{tab}', Static)
                if empty:
                    label = {'unread': 'No unread events',
                             'read': 'No read events',
                             'all': f'No events in last {self.lookback_days}d'}
                    es.update(label.get(tab, 'No events'))
                es.display = empty
            except Exception:
                pass
        self._update_title()

    def _update_title(self) -> None:
        recent        = [g for g in self._all_groups
                         if _is_recent(g) and g.get('state') not in self._EVENT_EXCLUDE]
        unread_groups = [g for g in recent if self._group_is_unread(g)]
        unread        = len(unread_groups)
        read_count    = len(recent) - unread
        self.post_message(self.UnreadCount(unread))

        failed    = sum(1 for g in unread_groups if g['state'] in ('FAILED', 'OUT_OF_MEMORY'))
        timeout   = sum(1 for g in unread_groups if g['state'] == 'TIMEOUT')
        completed = sum(1 for g in unread_groups if g['state'] == 'COMPLETED')
        self.post_message(self.StatusCounts(failed, timeout, completed))

        if unread:
            self.border_title = f'Events  [bold]{unread}[/bold] [dim]unread[/dim]'
        else:
            self.border_title = 'Events'

    def _build_row(
        self, g: dict, state_style: dict,
        c_muted: str, c_secondary: str, tv: dict,
        expanded: bool = False, time_mode: str = 'relative',
    ) -> tuple:
        """Build a single DataTable row tuple for group g."""
        unread = self._group_is_unread(g)
        _time_mode = time_mode

        def _cell(content: str, w: int, style: str = '') -> Text:
            return Text(content, style=style)

        _TERMINAL = {'COMPLETED', 'FAILED', 'TIMEOUT', 'OUT_OF_MEMORY', 'CANCELLED'}
        name_style    = 'bold' if unread else c_muted
        n             = len(g['ids'])
        if expanded:
            fold = '▼'
        elif n > 1:
            fold = '▶'
        else:
            fold = ''
        count_str     = str(n)
        disp_name     = f'   {fold} {g["disp_name"]}' if fold else f'     {g["disp_name"]}'
        elapsed_style = (f'bold {state_style.get(g["state"], c_muted)}'
                         if g['state'] in _TERMINAL else c_muted)
        gpu_label = _fmt_gpu(g['gpu_models']) or g['part']

        has_oom = any(jid in self._oom_jobs for jid in g['ids'])
        if has_oom:
            badge = state_badge('OUT_OF_MEMORY')
        else:
            badge = _type_badge(g['state'], tv)

        gpu_total = g.get('gpu_count', 0)
        gpu_count_str = str(gpu_total) if gpu_total else ''
        return (
            _cell(disp_name,              _COL_WIDTHS['name'],    name_style),
            badge,
            _cell(_rel_time(g['start'], _time_mode),  _COL_WIDTHS['ago'],     c_secondary),
            _cell(count_str,              _COL_WIDTHS['count'],   f'bold {c_muted}'),
            Text(gpu_count_str, style=f'bold {c_muted}'),
            _cell(gpu_label,              _COL_WIDTHS['gpu'],     c_secondary),
            _cell(_fmt_nodes(g['nodes']), _COL_WIDTHS['nodes'],   c_secondary),
            _cell(g['elapsed'],           _COL_WIDTHS['elapsed'], elapsed_style),
            _cell(_fmt_ids(g['ids']),     _COL_WIDTHS['ids'],     c_muted),
        )

    def _build_individual_row(
        self, row_data: Tuple, state_style: dict,
        c_muted: str, c_secondary: str, tv: dict,
        time_mode: str = 'relative', is_last: bool = False,
    ) -> tuple:
        """Build a DataTable row for an individual job within an expanded group."""
        jid, name, part, start, elapsed, state = (list(row_data) + [''] * 6)[:6]
        alloc_tres = row_data[7] if len(row_data) > 7 else ''
        nodelist   = row_data[8] if len(row_data) > 8 else ''
        gpu_label  = _parse_gpu(alloc_tres) or part
        state_base = state.split()[0] if state else ''
        branch = '└' if is_last else '├'
        ind_gpu = _parse_gpu_count(alloc_tres)
        return (
            Text(f'   {branch}── {name}', style=c_muted),
            _type_badge(state_base, tv),
            Text(_rel_time(start, time_mode), style=c_secondary),
            Text('',                        style=c_muted),
            Text(str(ind_gpu) if ind_gpu else '', style=c_muted),
            Text(gpu_label,                 style=c_secondary),
            Text(nodelist,                  style=c_secondary),
            Text(elapsed,                   style=c_muted),
            Text(jid,                       style=c_muted),
        )

    def _stats_text(self, groups: List[dict], tab: str) -> Text:
        tv = self.app.theme_variables
        c_muted   = tc(tv, 'text-muted',   'bright_black')
        c_error   = tc(tv, 'text-error',   'red')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')

        counts: Dict[str, int] = defaultdict(int)
        total_jobs = 0
        for g in groups:
            counts[g['state']] += len(g['ids'])
            total_jobs += len(g['ids'])

        if not groups:
            label = {'unread': 'no unread events', 'read': 'no read events', 'all': f'no events in last {self.lookback_days}d'}
            return Text(label.get(tab, 'no events'))

        t = Text()
        ordered = [
            ('FAILED',        c_error,   'failed'),
            ('TIMEOUT',       c_error,   'timeout'),
            ('OUT_OF_MEMORY', c_error,   'oom'),
            ('CANCELLED',     c_warning, 'cancelled'),
            ('RUNNING',       c_success, 'running'),
            ('COMPLETED',     '#4A9FD9',   'completed'),
            ('PENDING',       c_warning, 'pending'),
        ]
        parts = []
        for state, color, label in ordered:
            n = counts.get(state, 0)
            if n:
                chunk = Text()
                chunk.append(str(n), style=f'bold {color}')
                chunk.append(f' {label}', style=c_muted)
                parts.append(chunk)

        for i, part in enumerate(parts):
            t.append_text(part)
            if i < len(parts) - 1:
                t.append('  ', style=c_muted)

        n_groups = len(groups)
        t.append(f'  ({n_groups} group{"s" if n_groups != 1 else ""}, {total_jobs} job{"s" if total_jobs != 1 else ""})',
                 style=c_muted)
        n_oom = len(self._oom_jobs)
        if n_oom:
            t.append('  ')
            t.append(f'☢ {n_oom} OOM', style=f'bold {c_error}')
        return t

    def _selected_first_jid(self) -> Optional[str]:
        dt = self._active_dt()
        key = self._selected_key(dt, self._ctx)
        if not key:
            return None
        if key.startswith(_DIV_KEY_PREFIX):
            return None
        if key.startswith(_IND_KEY_PREFIX):
            return key[len(_IND_KEY_PREFIX):]
        return key

    def _selected_raw_key(self) -> Optional[str]:
        """Return the raw row key without stripping any prefix."""
        dt = self._active_dt()
        return self._selected_key(dt, self._ctx)

    def _set_read(self, first_jid: str, read: bool) -> None:
        for jid in self._first_to_all.get(first_jid, [first_jid]):
            if read:
                self._read_ids.add(jid)
            else:
                self._read_ids.discard(jid)
        _save_read_ids(self._read_ids, getattr(self.app, '_max_read_ids', 2000))
        self._refresh_all_tables()

    def _mark_all_read(self) -> None:
        for r in self._all_rows:
            self._read_ids.add(r[0])
        _save_read_ids(self._read_ids, getattr(self.app, '_max_read_ids', 2000))
        self._refresh_all_tables()

    def _mark_all_unread(self) -> None:
        for r in self._all_rows:
            self._read_ids.discard(r[0])
        _save_read_ids(self._read_ids, getattr(self.app, '_max_read_ids', 2000))
        self._refresh_all_tables()

    def action_toggle_read(self) -> None:
        fid = self._selected_first_jid()
        if fid:
            currently_unread = self._group_is_unread(
                next((g for g in self._all_groups if g['first_jid'] == fid), {'ids': [fid]})
            )
            self._set_read(fid, read=currently_unread)

    def action_mark_all(self) -> None:
        """Mark all as read (from unread/all tab) or all as unread (from read tab)."""
        tab = self.query_one(_HISTORY_TC, TabbedContent).active.removeprefix('tc-')
        if tab == 'read':
            self._mark_all_unread()
        else:
            self._mark_all_read()

    def action_refresh(self) -> None:
        self._load()

    def action_full_history(self) -> None:
        if not self._all_groups:
            return
        self.app.push_screen(
            FullHistoryModal(self._all_groups, self._build_row, self._state_style_dict)
        )

    def action_fold_all(self) -> None:
        """Fold or unfold ALL groups."""
        tab = self._active_tab()
        dt = self._active_dt()
        tree = self._trees.get(tab, [])
        self._fold_all_and_rebuild(dt, self._ctx, tree, FoldMode.EXPANDED_SET)
        # Re-render other tabs too since they share expanded state
        self._refresh_all_tables()

    def action_toggle_all_read(self) -> None:
        """Toggle read/unread for ALL visible events."""
        tab = self.query_one(_HISTORY_TC, TabbedContent).active.removeprefix('tc-')
        groups = self._filtered_groups(tab)
        all_read = all(not self._group_is_unread(g) for g in groups)
        for g in groups:
            for jid in g['ids']:
                if all_read:
                    self._read_ids.discard(jid)
                else:
                    self._read_ids.add(jid)
        _save_read_ids(self._read_ids, getattr(self.app, '_max_read_ids', 2000))
        self._refresh_all_tables()
        self._update_title()

    def action_expand_group(self) -> None:
        key = self._selected_raw_key()
        if not key or key.startswith(_DIV_KEY_PREFIX) or key.startswith(_IND_KEY_PREFIX):
            return
        if len(self._first_to_all.get(key, [key])) <= 1:
            return
        tab = self._active_tab()
        dt = self._active_dt()
        tree = self._trees.get(tab, [])
        self._toggle_and_rebuild(dt, self._ctx, tree, key)
        # Re-render other tabs since they share expanded state
        self._refresh_all_tables()

    def action_view_log(self) -> None:
        fid = self._selected_first_jid()
        if not fid:
            return
        self.dismiss_fresh(fid)
        tab = self.query_one(_HISTORY_TC, TabbedContent).active.removeprefix('tc-')
        groups = self._filtered_groups(tab)
        job_ids = [g['first_jid'] for g in groups if not g['first_jid'].startswith(_DIV_KEY_PREFIX)]
        current_idx = job_ids.index(fid) if fid in job_ids else 0

        sacct_ok = (getattr(self.app, '_cmd_sacct', True)
                    and getattr(self.app, '_feat_sacct_details', True))

        def _fetch_and_show() -> None:
            from speek.speek_max.slurm import fetch_job_details_and_log_path
            from speek.speek_max.log_scan import scan_log_incremental
            details, path = fetch_job_details_and_log_path(fid, sacct_fallback=sacct_ok)
            content, _ = scan_log_incremental(path, 0, 500) if path else (None, 0)
            from speek.speek_max.widgets.job_info_modal import JobInfoModal
            self.app.call_from_thread(
                lambda: self.app.push_screen(
                    JobInfoModal(fid, path, content, details, job_ids, current_idx)
                )
            )

        self.run_worker(_fetch_and_show, thread=True, group='log-view')

    def action_relaunch(self) -> None:
        """Re-submit the selected failed/timeout job using its original sbatch command."""
        fid = self._selected_first_jid()
        if not fid:
            return
        # Find the group to check state
        group = None
        for g in self._all_groups:
            if g['first_jid'] == fid:
                group = g
                break
        if not group:
            return
        state = group.get('state', '')
        if state not in ('FAILED', 'TIMEOUT', 'OUT_OF_MEMORY', 'CANCELLED'):
            self.app.notify(
                f'Job {fid} is {state} -- only failed/timeout/cancelled jobs can be relaunched',
                severity='warning',
            )
            return

        def _fetch_and_relaunch() -> None:
            import subprocess
            # Get the original submit command from sacct
            try:
                out = subprocess.check_output(
                    ['sacct', '-j', fid, '--parsable2', '--noheader',
                     '--format=SubmitLine', '-X'],
                    text=True, stderr=subprocess.DEVNULL, timeout=5,
                )
                submit_line = out.strip().splitlines()[0].strip() if out.strip() else ''
            except Exception:
                submit_line = ''

            if not submit_line:
                self.app.call_from_thread(
                    self.app.notify,
                    f'Could not find submit command for job {fid}',
                    severity='error',
                )
                return

            # Confirm before relaunching
            from speek.speek_max.widgets.confirmation import ConfirmationModal

            def _on_confirm(confirmed: bool) -> None:
                if not confirmed:
                    return

                def _run() -> None:
                    try:
                        result = subprocess.check_output(
                            submit_line, shell=True, text=True,
                            stderr=subprocess.STDOUT, timeout=30,
                        )
                        self.app.call_from_thread(
                            self.app.notify,
                            result.strip() or 'Submitted',
                            title='Relaunched',
                        )
                        # Refresh data
                        self.app.call_from_thread(self._load)
                    except subprocess.CalledProcessError as e:
                        msg = (e.output or str(e)).strip()
                        self.app.call_from_thread(
                            self.app.notify, msg,
                            title='Relaunch failed', severity='error',
                        )

                self.run_worker(_run, thread=True, group='relaunch')

            self.app.call_from_thread(
                lambda: self.app.push_screen(
                    ConfirmationModal(f'Relaunch job {fid}?\n\n{submit_line}'),
                    _on_confirm,
                )
            )

        self.run_worker(_fetch_and_relaunch, thread=True, group='relaunch-fetch')

    def _set_tab(self, tab_id: str) -> None:
        self.query_one(_HISTORY_TC, TabbedContent).active = tab_id

    def action_tab_unread(self) -> None:
        self._set_tab('tc-unread')

    def action_tab_read(self) -> None:
        self._set_tab('tc-read')

    def action_tab_all(self) -> None:
        self._set_tab('tc-all')
