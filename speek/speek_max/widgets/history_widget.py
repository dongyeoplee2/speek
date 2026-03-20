"""history_widget.py — sacct job history panel."""
from __future__ import annotations

import json
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

from speek.speek_max.slurm import fetch_history
from speek.speek_max._utils import tc, safe
from speek.speek_max.widgets.datatable import SpeekDataTable

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


_HISTORY_COL_WIDTHS = [3, 3, 2, 12, 6, 5, 7, 7, 8]

def _divider_row(age: str, date: str, n_cols: int = 9) -> tuple:
    """Return a tuple of Text cells for a time-divider separator row.

    age goes in the Ago column (1), date goes in Name column (3).
    """
    s = 'bold'
    cells = [Text('') for _ in range(n_cols)]
    cells[1] = Text(age, style=s)
    cells[3] = Text(f'▎{date}', style=s)
    return tuple(cells)


def _parse_gpu(alloc_tres: str) -> str:
    """Return gpu model name from AllocTRES string (e.g. 'gres/gpu:a100=2' → 'a100')."""
    m = _GPU_RE.search(alloc_tres)
    if m and m.group(1):
        return m.group(1).lower()
    return ''


def _fmt_gpu(models: set) -> str:
    return ','.join(sorted(m for m in models if m)) or ''


def _fmt_nodes(nodes: set) -> str:
    return ','.join(sorted(n for n in nodes if n and n != 'None')) or ''


# Maps state → (letter, bg_tv_key, bg_fallback)
# Badge renders as: dark text ON colored background
_TYPE_BADGE: Dict[str, Tuple[str, str, str]] = {
    'COMPLETED':     ('C', 'primary',  'blue'),
    'FAILED':        ('F', 'error',    'red'),
    'TIMEOUT':       ('T', 'warning',  'yellow'),
    'CANCELLED':     ('X', 'text-muted', 'bright_black'),
    'OUT_OF_MEMORY': ('M', 'error',    'red'),
    'PENDING':       ('P', 'warning',  'yellow'),
    'RUNNING':       ('S', 'success',  'green'),
}

# (col_header, width) — order matches _setup_dt and _populate_dt
_COL_WIDTHS: Dict[str, int] = {
    'ago': 3, 'name': 12, 'gpu': 6, 'nodes': 5, 'count': 2, 'state': 7, 'elapsed': 7, 'ids': 8,
}
_N_HISTORY_COLS = 9  # E + Ago + # + Name + GPU + Nodes + State + Elapsed + IDs


def _type_badge(state: str, tv: dict) -> Text:
    """Return a symbol badge with colored background for the given job state."""
    normalized = state.split()[0] if state else ''
    entry = _TYPE_BADGE.get(normalized)
    if entry:
        symbol, bg_key, bg_fb = entry
        bg = tc(tv, bg_key, bg_fb)
        fg = tc(tv, 'background', 'black')
        return Text(f' {symbol} ', style=f'bold {fg} on {bg}')
    return Text(' ? ', style='dim')


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
    """Compact range notation: 1234-1238,1241+2…"""
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
            result += f'+{len(parts) - i}…'
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
               state: str, tb: int, gpu_model: str, nodelist: str) -> dict:
    return {
        'first_jid': jid, 'disp_name': name, 'part': part, 'state': state,
        'start': start, 'elapsed': elapsed, 'tb': tb, 'ids': [jid],
        'gpu_models': {gpu_model} if gpu_model else set(),
        'nodes': {nodelist} if nodelist and nodelist != 'None' else set(),
    }


def _merge_into(g: dict, jid: str, start: str, elapsed: str,
                gpu_model: str, nodelist: str) -> None:
    g['ids'].append(jid)
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
        matched = _find_matching_group(grouped, part, state, tb, name)
        if matched is None:
            grouped.append(_new_group(jid, name, part, start, elapsed,
                                      state, tb, gpu_model, nodelist))
        else:
            _merge_into(matched, jid, start, elapsed, gpu_model, nodelist)

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
        dt.add_column('E',       width=3)
        dt.add_column('Ago',     width=3)
        dt.add_column('#',       width=2)
        dt.add_column('Name',    width=12)
        dt.add_column('GPU',     width=6)
        dt.add_column('Nodes',   width=5)
        dt.add_column('State',   width=7)
        dt.add_column('Elapsed', width=7)
        dt.add_column('IDs',     width=8)
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
                dt.add_row(*_divider_row(_age, _date, _N_HISTORY_COLS), key=f'{_DIV_KEY_PREFIX}{div_counter}')
                div_counter += 1
            time_mode = getattr(self.app, '_time_format', 'relative')
            row = self._build_row(g, state_style, c_muted, c_secondary, tv, time_mode=time_mode)
            dt.add_row(*row, key=g['first_jid'])


class HistoryWidget(Widget):
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
        Binding('v',     'expand_group', '',        show=False),
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
        self._expanded_groups: set[str]        = set()
        self._oom_jobs: set[str]              = set()  # jids with OOM in logs
        self._oom_notified: set[str]          = set()
        # fresh_jids: job_id → monotonic timestamp when it became fresh
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
        """Return 0.0–1.0 highlight intensity for a fresh job. 0 = expired."""
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
                    yield SpeekDataTable(id='dt-unread', cursor_type='row', show_cursor=True)
                    yield Static('', id='stats-unread', classes='history-stats')
                with TabPane('2 Read', id='tc-read'):
                    yield SpeekDataTable(id='dt-read', cursor_type='row', show_cursor=True)
                    yield Static('', id='stats-read', classes='history-stats')
                with TabPane('3 All', id='tc-all'):
                    yield SpeekDataTable(id='dt-all', cursor_type='row', show_cursor=True)
                    yield Static('', id='stats-all', classes='history-stats')

    def on_mount(self) -> None:
        import time as _time
        self.border_title = 'Events'
        self._startup_wall = _time.time()
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
        dt.add_column('E',       width=3)
        dt.add_column('Ago',     width=3)
        dt.add_column('#',       width=2)
        dt.add_column('Name',    width=12)
        dt.add_column('GPU',     width=6)
        dt.add_column('Nodes',   width=5)
        dt.add_column('State',   width=7)
        dt.add_column('Elapsed', width=7)
        dt.add_column('IDs',     width=8)

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
        if not getattr(self.app, '_cmd_sacct', True):
            return
        if not getattr(self.app, '_feat_history', True):
            return
        self.query_one(LoadingIndicator).display = True
        days = self.lookback_days
        self.run_worker(
            lambda: fetch_history(days),
            thread=True, exclusive=True, group='history',
        )

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.worker.group == 'history' and event.state == WorkerState.SUCCESS:
            rows = event.worker.result
            self._all_rows = list(reversed(rows))
            self._jid_to_row = {row[0]: row for row in rows}
            self._all_groups, self._first_to_all = _aggregate(self._all_rows)
            self._expanded_groups &= set(self._first_to_all.keys())
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

    def _scan_oom(self) -> None:
        """Kick off a background worker to scan recent job logs for OOM."""
        # Collect jids of COMPLETED/RUNNING groups not yet scanned
        candidates = []
        for g in self._all_groups:
            if g['state'] in ('COMPLETED', 'RUNNING'):
                for jid in g['ids']:
                    if jid not in self._oom_jobs and jid not in self._oom_notified:
                        candidates.append(jid)
        if not candidates:
            return
        # Limit to most recent 30 to keep it fast
        candidates = candidates[:30]

        def _worker():
            from speek.speek_max.slurm import get_job_log_path
            from speek.speek_max.log_scan import detect_oom
            oom_found: set[str] = set()
            for jid in candidates:
                path = get_job_log_path(jid)
                if path and detect_oom(path):
                    oom_found.add(jid)
            return oom_found

        self.run_worker(_worker, thread=True, group='oom-scan')

    def _active_dt(self) -> SpeekDataTable:
        tc_widget = self.query_one(_HISTORY_TC, TabbedContent)
        dt_id = _DT_IDS.get(tc_widget.active.removeprefix('tc-'), 'dt-unread')
        return self.query_one(f'#{dt_id}', SpeekDataTable)

    def _group_is_unread(self, g: dict) -> bool:
        return any(jid not in self._read_ids for jid in g['ids'])

    def _filtered_groups(self, tab: str) -> List[dict]:
        recent = [g for g in self._all_groups if _is_recent(g)]
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
            'COMPLETED':     f'dim {c_muted}',
            'FAILED':        f'bold {c_error}',
            'TIMEOUT':       f'bold {c_error}',
            'CANCELLED':     f'dim {c_error}',
            'OUT_OF_MEMORY': f'bold {c_error}',
            'RUNNING':       f'bold {c_success}',
            'PENDING':       c_warning,
        }

    @safe('Events refresh')
    def _refresh_all_tables(self) -> None:
        for tab, dt_id in _DT_IDS.items():
            dt    = self.query_one(f'#{dt_id}', SpeekDataTable)
            stats = self.query_one(f'#stats-{tab}', Static)
            groups = self._filtered_groups(tab)
            self._populate_dt(dt, groups)
            stats.update(self._stats_text(groups, tab))
            empty = len(groups) == 0
            dt.display = not empty
            stats.set_class(empty, 'history-empty')
        self._update_title()

    def _update_title(self) -> None:
        recent        = [g for g in self._all_groups if _is_recent(g)]
        unread_groups = [g for g in recent if self._group_is_unread(g)]
        unread        = len(unread_groups)
        read_count    = len(recent) - unread
        self.post_message(self.UnreadCount(unread))

        failed    = sum(1 for g in unread_groups if g['state'] in ('FAILED', 'OUT_OF_MEMORY'))
        timeout   = sum(1 for g in unread_groups if g['state'] == 'TIMEOUT')
        completed = sum(1 for g in unread_groups if g['state'] == 'COMPLETED')
        self.post_message(self.StatusCounts(failed, timeout, completed))

        if unread or read_count:
            self.border_title = f'Events  [dim]{unread}U · {read_count}R[/dim]'
        else:
            self.border_title = 'Events'

    def _add_expanded_rows(self, dt: SpeekDataTable, g: dict,
                           state_style: dict, c_muted: str, c_secondary: str,
                           tv: dict) -> None:
        """Add individual job rows beneath an expanded group."""
        time_mode = getattr(self.app, '_time_format', 'relative')
        for jid in g['ids']:
            row_data = self._jid_to_row.get(jid)
            if row_data:
                ind = self._build_individual_row(
                    row_data, state_style, c_muted, c_secondary, tv, time_mode)
                dt.add_row(*ind, key=f'{_IND_KEY_PREFIX}{jid}')

    @safe('Events populate')
    def _populate_dt(self, dt: SpeekDataTable, groups: List[dict]) -> None:
        tv          = self.app.theme_variables
        c_muted     = tc(tv, 'text-muted',     'bright_black')
        c_secondary = tc(tv, 'text-secondary', 'default')
        state_style = self._state_style_dict(tv)

        cursor_key = None
        try:
            if dt.row_count > 0:
                cursor_key = dt.coordinate_to_cell_key(dt.cursor_coordinate)[0].value
        except Exception:
            pass

        time_mode = getattr(self.app, '_time_format', 'relative')

        with self.app.batch_update():
            dt.clear()
            current_zone = -1
            div_counter = 0
            for g in groups:
                zone = _time_zone_idx(g['start'])
                if zone != current_zone:
                    if current_zone >= 0:
                        dt.add_row(*[Text('') for _ in range(_N_HISTORY_COLS)], key=f'{_DIV_KEY_PREFIX}{div_counter}_sp')
                    current_zone = zone
                    _age, _date = _time_zone_parts(zone)
                    dt.add_row(*_divider_row(_age, _date, _N_HISTORY_COLS),
                               key=f'{_DIV_KEY_PREFIX}{div_counter}')
                    div_counter += 1
                expanded = g['first_jid'] in self._expanded_groups
                row = self._build_row(g, state_style, c_muted, c_secondary, tv,
                                      expanded=expanded, time_mode=time_mode)
                dt.add_row(*row, key=g['first_jid'])
                if expanded:
                    self._add_expanded_rows(dt, g, state_style, c_muted, c_secondary, tv)

        if cursor_key and not str(cursor_key).startswith(_DIV_KEY_PREFIX):
            try:
                dt.move_cursor(row=dt.get_row_index(cursor_key))
            except Exception:
                pass

    def _build_row(
        self, g: dict, state_style: dict,
        c_muted: str, c_secondary: str, tv: dict,
        expanded: bool = False, time_mode: str = 'relative',
    ) -> tuple:
        """Build a single DataTable row tuple for group g."""
        unread = self._group_is_unread(g)
        _time_mode = time_mode

        def _cell(content: str, w: int, style: str = '') -> Text:
            return Text(content[:w], style=style)

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
        disp_name     = f'{fold} {g["disp_name"]}' if fold else g['disp_name']
        elapsed_style = (f'bold {state_style.get(g["state"], c_muted)}'
                         if g['state'] in _TERMINAL else c_muted)
        gpu_label = _fmt_gpu(g['gpu_models']) or g['part']

        has_oom = any(jid in self._oom_jobs for jid in g['ids'])
        if has_oom:
            c_error = tc(tv, 'text-error', 'red')
            bg = tc(tv, 'error', 'red')
            fg = tc(tv, 'background', 'black')
            badge = Text(' ☠ ', style=f'bold {fg} on {bg}')
            state_cell = _cell('OOM', _COL_WIDTHS['state'], f'bold {c_error}')
        else:
            badge = _type_badge(g['state'], tv)
            state_cell = _cell(g['state'], _COL_WIDTHS['state'], state_style.get(g['state'], 'default'))

        return (
            badge,
            _cell(_rel_time(g['start'], _time_mode),  _COL_WIDTHS['ago'],     c_secondary),
            _cell(count_str,              _COL_WIDTHS['count'],   f'bold {c_muted}'),
            _cell(disp_name,              _COL_WIDTHS['name'],    name_style),
            _cell(gpu_label,              _COL_WIDTHS['gpu'],     c_secondary),
            _cell(_fmt_nodes(g['nodes']), _COL_WIDTHS['nodes'],   c_secondary),
            state_cell,
            _cell(g['elapsed'],           _COL_WIDTHS['elapsed'], elapsed_style),
            _cell(_fmt_ids(g['ids']),     _COL_WIDTHS['ids'],     c_muted),
        )

    def _build_individual_row(
        self, row_data: Tuple, state_style: dict,
        c_muted: str, c_secondary: str, tv: dict,
        time_mode: str = 'relative',
    ) -> tuple:
        """Build a DataTable row for an individual job within an expanded group."""
        jid, name, part, start, elapsed, state = (list(row_data) + [''] * 6)[:6]
        alloc_tres = row_data[7] if len(row_data) > 7 else ''
        nodelist   = row_data[8] if len(row_data) > 8 else ''
        gpu_label  = _parse_gpu(alloc_tres) or part
        state_base = state.split()[0] if state else ''
        return (
            _type_badge(state_base, tv),
            Text(_rel_time(start, time_mode)[:8], style=c_secondary),
            Text('  ↳',                     style=c_muted),
            Text(f'  {name}'[:18],          style=c_muted),
            Text(gpu_label[:12],            style=c_secondary),
            Text(nodelist[:10],             style=c_secondary),
            Text(state_base[:12],           style=state_style.get(state_base, c_muted)),
            Text(elapsed[:9],               style=c_muted),
            Text(jid[:18],                  style=c_muted),
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
            ('COMPLETED',     c_muted,   'completed'),
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
            t.append(f'☠ {n_oom} OOM', style=f'bold {c_error}')
        return t

    def _selected_first_jid(self) -> Optional[str]:
        dt = self._active_dt()
        if dt.row_count == 0:
            return None
        try:
            row_key, _ = dt.coordinate_to_cell_key(dt.cursor_coordinate)
            key = str(row_key.value)
            if key.startswith(_DIV_KEY_PREFIX):
                return None
            if key.startswith(_IND_KEY_PREFIX):
                return key[len(_IND_KEY_PREFIX):]
            return key
        except Exception:
            return None

    def _selected_raw_key(self) -> Optional[str]:
        """Return the raw row key without stripping any prefix."""
        dt = self._active_dt()
        if dt.row_count == 0:
            return None
        try:
            row_key, _ = dt.coordinate_to_cell_key(dt.cursor_coordinate)
            return str(row_key.value)
        except Exception:
            return None

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
        """Fold or unfold ALL groups. If any open, fold all. If all folded, unfold all."""
        expandable = {g['first_jid'] for g in self._all_groups
                      if len(self._first_to_all.get(g['first_jid'], [])) > 1}
        if expandable - self._expanded_groups:
            # Some are collapsed → expand all
            self._expanded_groups |= expandable
        else:
            # All expanded → collapse all
            self._expanded_groups.clear()
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
        if key in self._expanded_groups:
            self._expanded_groups.discard(key)
        else:
            self._expanded_groups.add(key)
        self._refresh_all_tables()
        try:
            self._active_dt().move_cursor(row=self._active_dt().get_row_index(key))
        except Exception:
            pass

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
                f'Job {fid} is {state} — only failed/timeout/cancelled jobs can be relaunched',
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
