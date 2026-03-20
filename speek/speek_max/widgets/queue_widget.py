"""queue_widget.py — Full cluster queue panel."""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.widget import Widget
from textual.widgets import Label, LoadingIndicator, Static

from speek.speek_max.slurm import fetch_all_priorities, fetch_job_details, fetch_job_stats, fetch_queue
from speek.speek_max._utils import tc
from speek.speek_max.widgets.datatable import SpeekDataTable
from speek.speek_max.widgets.ping_tracker import PingTracker


def _color_flash(cell: Text, intensity: float) -> Text:
    """Flash a changed cell's text to bright white, fading back to original color."""
    if intensity <= 0:
        return cell
    # Blend towards white based on intensity
    bright = int(180 + intensity * 75)  # 180–255 range
    flash_color = f'#{bright:02x}{bright:02x}{bright:02x}'
    return Text(cell.plain, style=f'bold {flash_color}')


_RANK_EMOJI = {1: '🥇', 2: '🥈', 3: '🥉'}


_TRAIL_RE = re.compile(r'[\d_\-\.]+$')

def _name_base(name: str) -> str:
    """Strip trailing numbers/separators: 'train_run_007' → 'train_run_'"""
    return _TRAIL_RE.sub('', name)

def _should_merge(a: str, b: str) -> bool:
    """True if two job names are similar enough to group together."""
    if a == b:
        return True
    ba, bb = _name_base(a), _name_base(b)
    return bool(ba and ba == bb)


def _pre_build_queue_rows(
    rows: List[Tuple], priorities: Optional[Dict], tv: dict,
) -> Tuple[frozenset, List[Tuple], Optional['Text'], bool]:
    """Aggregate + pre-build Rich Text rows for the queue DataTable.
    Runs in a worker thread so the main thread only does DataTable DOM ops."""
    c_muted     = tc(tv, 'text-muted',     'bright_black')
    c_primary   = tc(tv, 'text-primary',   'bold')
    c_secondary = tc(tv, 'text-secondary', 'default')
    c_warning   = tc(tv, 'text-warning',   'yellow')

    # Structural signature for skip-rebuild check (excludes elapsed)
    sig = frozenset((r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows)

    # Aggregation
    groups: Dict[tuple, dict] = {}
    for jid, user, name, part, gpus, state, elapsed in rows:
        matched = None
        for key, g in groups.items():
            ku, kn, kp, ks = key
            if ku == user and kp == part and ks == state and _should_merge(kn, name):
                matched = key
                break
        if matched is None:
            matched = (user, name, part, state)
            groups[matched] = {'ids': [], 'gpus': 0, 'elapsed': elapsed,
                               'first_id': jid, 'name': name}
        g = groups[matched]
        g['ids'].append(jid)
        try:
            g['gpus'] += int(gpus)
        except (ValueError, TypeError):
            pass
        if elapsed > g['elapsed']:
            g['elapsed'] = elapsed

    # Top-3 user rank by running GPUs
    user_running: Dict[str, int] = defaultdict(int)
    for (user, _, _, state), g in groups.items():
        if state == 'RUNNING':
            user_running[user] += g['gpus']
    ranked = [u for u, _ in sorted(user_running.items(), key=lambda x: -x[1]) if _ > 0]
    user_rank = {u: i + 1 for i, u in enumerate(ranked[:3])}

    # Pre-build Rich Text row tuples: (col0, col1, …, col8, row_key)
    built: List[Tuple] = []
    for (user, _kn, part, state), g in groups.items():
        count   = len(g['ids'])
        gpu_str = str(g['gpus']) if g['gpus'] else '-'
        emoji   = _RANK_EMOJI.get(user_rank.get(user, 0), '')
        user_cell = Text()
        user_cell.append(emoji)
        user_cell.append(user, style=f'bold {c_primary}')
        prio_text = Text('-', style=c_muted)
        if state == 'PENDING' and priorities:
            entry = priorities.get(g['first_id'])
            if entry:
                try:
                    prio_text = Text(str(int(float(entry.get('total', 0)))),
                                     style=c_warning)
                except (ValueError, TypeError):
                    pass
        built.append((
            Text(str(count), style=f'bold {c_muted}'),
            user_cell,
            Text(g['name']),
            Text(part, style=c_secondary),
            Text(gpu_str, style='bold'),
            _state_text(state, tv),
            Text(g['elapsed'], style=c_muted),
            prio_text,
            Text(_fmt_job_ids(g['ids']), style=c_muted),
            g['first_id'],   # row key — NOT a column cell
        ))

    # Pre-build stats bar text
    stats_text: Optional[Text] = None

    return sig, built, stats_text, bool(rows)


def _fmt_job_ids(ids: List[str], max_len: int = 32) -> str:
    """Compact range notation: 1234-1238, 1241 (truncated to max_len chars)."""
    try:
        ints = sorted(int(i) for i in ids)
    except ValueError:
        return ', '.join(ids[:4])
    groups, start, prev = [], ints[0], ints[0]
    for x in ints[1:]:
        if x == prev + 1:
            prev = x
        else:
            groups.append((start, prev))
            start = prev = x
    groups.append((start, prev))
    parts = [str(a) if a == b else f'{a}-{b}' for a, b in groups]
    result, buf = '', ''
    for i, p in enumerate(parts):
        buf = (result + ', ' + p) if result else p
        if len(buf) > max_len:
            remaining = len(parts) - i
            result += f' +{remaining}…'
            break
        result = buf
    return result


def _state_text(state: str, tv: dict) -> Text:
    s = tc(tv, 'text-success', 'green')
    w = tc(tv, 'text-warning', 'yellow')
    e = tc(tv, 'text-error', 'red')
    m = tc(tv, 'text-muted', 'bright_black')
    styles = {
        'RUNNING': f'bold {s}',
        'PENDING': w,
        'FAILED': f'bold {e}',
        'TIMEOUT': f'bold {e}',
        'CANCELLED': f'dim {e}',
        'COMPLETED': f'dim {m}',
    }
    return Text(state, style=styles.get(state, 'default'))


class QueueWidget(Widget):
    """Full cluster queue (all users, RUNNING+PENDING). Auto-refreshes every 5s."""

    BORDER_TITLE = "Queue"
    can_focus = True

    BINDINGS = [
        Binding('d', 'job_detail', 'Detail', show=True),
        Binding('r', 'refresh', 'Refresh', show=True),
    ]

    def compose(self) -> ComposeResult:
        """Compose the queue widget."""
        yield LoadingIndicator()
        yield Static('', id='queue-empty', classes='empty-state')
        yield SpeekDataTable(id='queue-dt', cursor_type='row', show_cursor=True)
        yield Static('', id='queue-stats')

    def on_mount(self) -> None:
        self._last_queue_sig: frozenset = frozenset()
        ping_dur = getattr(self.app, '_ping_duration', 10)
        self._ping = PingTracker(duration=ping_dur)
        dt = self.query_one(SpeekDataTable)
        self._col_widths = [3, 12, 20, 10, 5, 9, 9, 6, 24]
        dt.zebra_stripes = True
        dt.add_column('#',         width=3)
        dt.add_column('User',      width=12)
        dt.add_column('Name',      width=20)
        dt.add_column('Partition', width=10)
        dt.add_column('GPU',       width=5)
        dt.add_column('State',     width=9)
        dt.add_column('Elapsed',   width=9)
        dt.add_column('Prio',      width=6)
        dt.add_column('IDs',       width=24)

    def on_show(self) -> None:
        self._load()
        if not hasattr(self, '_interval_started'):
            self._interval_started = True
            interval = getattr(self.app, '_queue_refresh', 5)
            self._refresh_timer = self.set_interval(interval, self._load)

    def set_refresh_interval(self, seconds: int) -> None:
        try:
            self._refresh_timer.stop()
        except Exception:
            pass
        self._refresh_timer = self.set_interval(seconds, self._load)

    def on_click(self, event) -> None:
        try:
            self.query_one(SpeekDataTable).focus()
        except Exception:
            pass

    def _load(self) -> None:
        if not getattr(self.app, '_cmd_squeue', True):
            return
        tv = self.app.theme_variables  # snapshot on main thread before worker starts
        self.run_worker(
            lambda: self._fetch(tv), thread=True, exclusive=True, group='queue',
        )

    def _fetch(self, tv: dict) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        rows       = fetch_queue()
        job_stats  = fetch_job_stats()
        priorities = fetch_all_priorities()
        if worker.is_cancelled:
            return
        sig, built, _, has_rows = _pre_build_queue_rows(rows, priorities, tv)
        # Pre-build stats bar text in the worker too
        c_muted   = tc(tv, 'text-muted',   'bright_black')
        c_primary = tc(tv, 'text-primary', 'bold')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        stats_text = None
        if job_stats:
            t = Text()
            t.append(str(job_stats.get('total_R',  0)), style=f'bold {c_success}')
            t.append(' running  ',                      style=c_muted)
            t.append(str(job_stats.get('total_PD', 0)), style=f'bold {c_warning}')
            t.append(' pending  ',                      style=c_muted)
            t.append(str(job_stats.get('active_users', 0)), style=f'bold {c_primary}')
            t.append(' active users',                   style=c_muted)
            stats_text = t
        self.app.call_from_thread(self._update, sig, built, stats_text, has_rows)

    def _update(self, sig: frozenset, built: List[Tuple],
                stats_text: Optional[Text], has_rows: bool) -> None:
        if sig == self._last_queue_sig:
            return
        self._last_queue_sig = sig

        # Update ping tracker with per-cell signatures
        self._ping.duration = getattr(self.app, '_ping_duration', 10)
        row_sigs: dict[str, list[str]] = {}
        for *cells, row_key in built:
            row_sigs[str(row_key)] = [
                c.plain if hasattr(c, 'plain') else str(c) for c in cells
            ]
        self._ping.update(row_sigs)

        dt    = self.query_one(SpeekDataTable)
        empty = self.query_one('#queue-empty', Static)

        try:
            cursor_key = dt.coordinate_to_cell_key(dt.cursor_coordinate)[0].value if dt.row_count else None
        except Exception:
            cursor_key = None

        with self.app.batch_update():
            dt.clear()
            for *cells, row_key in built:
                rk = str(row_key)
                out = [
                    _color_flash(cells[i], self._ping.cell_intensity(rk, i))
                    for i in range(len(cells))
                ]
                dt.add_row(*out, key=row_key)

            # Ghost rows for recently removed entries
            for ghost_key, g_intensity in self._ping.ghosts():
                bright = int(60 + g_intensity * 80)
                gs = f'dim #{bright:02x}{bright:02x}{bright:02x}'
                n = len(self._col_widths)
                ghost = [Text('—', style=gs)] + [Text('', style=gs) for _ in range(n - 1)]
                dt.add_row(*ghost, key=f'_ghost_{ghost_key}')

        # Schedule re-render while pings are fading
        if self._ping.has_active and not hasattr(self, '_ping_timer'):
            self._ping_timer = self.set_interval(2, self._tick_ping)
        elif not self._ping.has_active and hasattr(self, '_ping_timer'):
            self._ping_timer.stop()
            del self._ping_timer

        self.query_one(LoadingIndicator).display = False
        if has_rows:
            empty.display = False
        else:
            empty.update('No jobs currently in queue')
            empty.display = True

        stats_bar = self.query_one('#queue-stats', Static)
        if stats_text is not None:
            stats_bar.update(stats_text)
            stats_bar.display = True
        else:
            stats_bar.display = False

        if cursor_key:
            try:
                dt.move_cursor(row=dt.get_row_index(cursor_key))
            except Exception:
                pass

    def _tick_ping(self) -> None:
        """Re-render table while ping highlights are fading."""
        if self._ping.has_active:
            # Force a refresh by invalidating the sig
            self._last_queue_sig = frozenset()
            self._load()
        elif hasattr(self, '_ping_timer'):
            self._ping_timer.stop()
            del self._ping_timer

    def _selected_job_id(self):
        dt = self.query_one(SpeekDataTable)
        if dt.row_count == 0:
            return None
        try:
            row_key, _ = dt.coordinate_to_cell_key(dt.cursor_coordinate)
            return str(row_key.value)
        except Exception:
            return None

    def action_job_detail(self) -> None:
        jid = self._selected_job_id()
        if not jid:
            return

        def _fetch() -> None:
            details = fetch_job_details(jid)
            self.app.call_from_thread(self._show_detail, jid, details)

        self.run_worker(_fetch, thread=True, group='job-detail')

    def _show_detail(self, jid: str, details: dict) -> None:
        from speek.speek_max.widgets.job_detail import JobDetailModal
        self.app.push_screen(JobDetailModal(jid, details))

    def action_refresh(self) -> None:
        self._load()
