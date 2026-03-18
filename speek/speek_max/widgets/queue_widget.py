"""queue_widget.py — Full cluster queue panel."""
from __future__ import annotations

import re
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.widget import Widget
from textual.widgets import Label, LoadingIndicator, Static

from speek.speek_max.slurm import fetch_all_priorities, fetch_job_details, fetch_job_stats, fetch_queue
from speek.speek_max._utils import tc
from speek.speek_max.widgets.datatable import SpeekDataTable


_RANK_EMOJI = {1: '🥇', 2: '🥈', 3: '🥉'}


_TRAIL_RE = re.compile(r'[\d_\-\.]+$')

def _name_base(name: str) -> str:
    """Strip trailing numbers/separators: 'train_run_007' → 'train_run_'"""
    return _TRAIL_RE.sub('', name)

def _should_merge(a: str, b: str, threshold: float = 0.82) -> bool:
    """True if two job names are similar enough to group together."""
    if a == b:
        return True
    # Fast path: same base after stripping trailing digits
    if _name_base(a) == _name_base(b) and _name_base(a):
        return True
    # Fallback: character-level similarity
    return SequenceMatcher(None, a, b).ratio() >= threshold


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
    s = tc(tv, 'text-success', '#00FA9A')
    w = tc(tv, 'text-warning', '#FFD700')
    e = tc(tv, 'text-error', '#FF4500')
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
        yield LoadingIndicator()
        yield Static('', id='queue-empty', classes='empty-state')
        yield SpeekDataTable(id='queue-dt', cursor_type='row', show_cursor=True)
        yield Static('', id='queue-stats')

    def on_mount(self) -> None:
        dt = self.query_one(SpeekDataTable)
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
            self.set_interval(5, self._load)

    def on_click(self, event) -> None:
        try:
            self.query_one(SpeekDataTable).focus()
        except Exception:
            pass

    def _load(self) -> None:
        self.run_worker(self._fetch, thread=True, exclusive=True, group='queue')

    def _fetch(self) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        rows = fetch_queue()
        job_stats = fetch_job_stats()
        priorities = fetch_all_priorities()
        if not worker.is_cancelled:
            self.app.call_from_thread(self._update, rows, job_stats, priorities)

    def _update(self, rows: List[Tuple], job_stats: Dict = None, priorities: Dict = None) -> None:
        self.query_one(LoadingIndicator).display = False
        empty = self.query_one('#queue-empty', Static)
        dt = self.query_one(SpeekDataTable)
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_primary = tc(tv, 'text-primary', 'bold')
        c_secondary = tc(tv, 'text-secondary', 'default')

        # Aggregate: group jobs with same user/partition/state and similar name
        groups: Dict[tuple, dict] = {}
        for jid, user, name, part, gpus, state, elapsed in rows:
            # Find existing group with matching user/part/state and similar name
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

        try:
            cursor_key = dt.coordinate_to_cell_key(dt.cursor_coordinate)[0].value if dt.row_count else None
        except Exception:
            cursor_key = None

        c_success = tc(tv, 'text-success', '#00FA9A')
        c_warning = tc(tv, 'text-warning', '#FFD700')

        # per-user running GPU totals → rank top 3
        user_running: Dict[str, int] = defaultdict(int)
        for (user, _, _, state), g in groups.items():
            if state == 'RUNNING':
                user_running[user] += g['gpus']
        ranked = [u for u, _ in sorted(user_running.items(), key=lambda x: -x[1]) if _ > 0]
        user_rank = {u: i + 1 for i, u in enumerate(ranked[:3])}

        with self.app.batch_update():
            dt.clear()
            for (user, _key_name, part, state), g in groups.items():
                count = len(g['ids'])
                gpu_str = str(g['gpus']) if g['gpus'] else '-'
                emoji = _RANK_EMOJI.get(user_rank.get(user, 0), '')
                user_cell = Text()
                user_cell.append(emoji)
                user_cell.append(user, style=f'bold {c_primary}')
                prio_text = Text('-', style=c_muted)
                if state == 'PENDING' and priorities:
                    prio_entry = priorities.get(g['first_id'])
                    if prio_entry:
                        try:
                            prio_val = int(float(prio_entry.get('total', 0)))
                            prio_text = Text(str(prio_val), style=c_warning)
                        except (ValueError, TypeError):
                            pass
                dt.add_row(
                    Text(str(count), style=f'bold {c_muted}'),
                    user_cell,
                    Text(g['name']),
                    Text(part, style=c_secondary),
                    Text(gpu_str, style='bold'),
                    _state_text(state, tv),
                    Text(g['elapsed'], style=c_muted),
                    prio_text,
                    Text(_fmt_job_ids(g['ids']), style=c_muted),
                    key=g['first_id'],
                )


        if not rows:
            empty.update('No jobs currently in queue')
            empty.display = True
        else:
            empty.display = False

        stats_bar = self.query_one('#queue-stats', Static)
        if job_stats:
            total_R  = job_stats.get('total_R', 0)
            total_PD = job_stats.get('total_PD', 0)
            active_u = job_stats.get('active_users', 0)
            t = Text()
            t.append(str(total_R),  style=f'bold {c_success}')
            t.append(' running  ',  style=c_muted)
            t.append(str(total_PD), style=f'bold {c_warning}')
            t.append(' pending  ',  style=c_muted)
            t.append(str(active_u), style=f'bold {c_primary}')
            t.append(' active users', style=c_muted)
            stats_bar.update(t)
            stats_bar.display = True
        else:
            stats_bar.display = False

        if cursor_key:
            try:
                dt.move_cursor(row=dt.get_row_index(cursor_key))
            except Exception:
                pass

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
