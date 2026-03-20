"""users_widget.py — Per-user GPU usage analysis from sacct + squeue."""
from __future__ import annotations

from typing import List, Dict

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label, LoadingIndicator, Static

from speek.speek_max.slurm import fetch_user_stats, fetch_fairshares
from speek.speek_max._utils import tc
from speek.speek_max.widgets.datatable import SpeekDataTable


def _fmt_hours(h: float) -> str:
    if h >= 1000:
        return f'{h/1000:.1f}kh'
    if h >= 1:
        return f'{h:.0f}h'
    return f'{h*60:.0f}m'


def _fmt_avg(secs: int) -> str:
    if secs <= 0:
        return '-'
    h, r = divmod(secs, 3600)
    m, _ = divmod(r, 60)
    if h >= 24:
        return f'{h//24}d {h%24}h'
    return f'{h}h{m:02d}m' if h else f'{m}m'


def _success_color(pct: float, tv: dict) -> str:
    if pct >= 90:
        return tc(tv, 'text-success', 'green')
    if pct >= 70:
        return tc(tv, 'text-warning', 'yellow')
    return tc(tv, 'text-error', 'red')


class UsersWidget(Widget):
    """Per-user GPU usage analysis (sacct history + live squeue)."""

    BORDER_TITLE = "Users"
    can_focus = True

    BINDINGS = [
        Binding('d', 'lookback_1d',  '1d',  show=True),
        Binding('w', 'lookback_7d',  '7d',  show=True),
        Binding('m', 'lookback_30d', '30d', show=True),
        Binding('r', 'action_refresh', 'Refresh', show=True),
    ]

    lookback_days: reactive[int] = reactive(30)

    def compose(self) -> ComposeResult:
        """Compose the users widget."""
        from textual.containers import Horizontal
        with Horizontal(id='users-toolbar'):
            yield Label('History:', id='users-lb-label')
            yield Label('[ 1d ]',  id='users-lb-1d')
            yield Label('[ 7d ]',  id='users-lb-7d')
            yield Label('[30d]',   id='users-lb-30d')
        yield LoadingIndicator()
        yield Static('', id='users-empty', classes='empty-state')
        yield SpeekDataTable(id='users-dt', cursor_type='row', show_cursor=True)

    def on_mount(self) -> None:
        dt = self.query_one(SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('User',      width=14)
        dt.add_column('Now GPU',   width=8)
        dt.add_column('Pending',   width=8)
        dt.add_column('Jobs',      width=6)
        dt.add_column('GPU-hrs',   width=8)
        dt.add_column('Success%',  width=9)
        dt.add_column('Fail',      width=5)
        dt.add_column('Avg dur',   width=8)
        dt.add_column('FairShare', width=10)
        dt.add_column('Partition', width=10)
        self._update_toolbar()

    def on_show(self) -> None:
        self._load()
        if not hasattr(self, '_interval_started'):
            self._interval_started = True
            self.set_interval(30, self._load)

    def on_click(self, event) -> None:
        try:
            self.query_one(SpeekDataTable).focus()
        except Exception:
            pass

    def watch_lookback_days(self, _old: int, _new: int) -> None:
        self._update_toolbar()
        self._load()

    def _update_toolbar(self) -> None:
        from speek.speek_max._utils import tcs
        tv = self.app.theme_variables
        active   = tcs(tv, 'primary',    '#C45AFF')
        inactive = tcs(tv, 'text-muted', 'ansi_bright_black')
        for days, wid in [(1, '#users-lb-1d'), (7, '#users-lb-7d'), (30, '#users-lb-30d')]:
            lbl = self.query_one(wid, Label)
            if self.lookback_days == days:
                lbl.styles.color = active
                lbl.styles.text_style = 'bold'
            else:
                lbl.styles.color = inactive
                lbl.styles.text_style = 'none'

    def _load(self) -> None:
        if not getattr(self.app, '_cmd_sacct', True):
            return
        days = self.lookback_days
        self.run_worker(
            lambda: (fetch_user_stats(days), fetch_fairshares()),
            thread=True, exclusive=True, group='users',
        )

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.worker.group == 'users' and event.state == WorkerState.SUCCESS:
            rows, fairshares = event.worker.result
            self._update(rows, fairshares)

    def _update(self, rows: List[Dict], fairshares: Dict = None) -> None:
        self.query_one(LoadingIndicator).display = False
        empty = self.query_one('#users-empty', Static)
        dt    = self.query_one(SpeekDataTable)
        tv    = self.app.theme_variables

        c_muted     = tc(tv, 'text-muted',    'bright_black')
        c_primary   = tc(tv, 'primary',       'magenta')
        c_success   = tc(tv, 'text-success',  'green')
        c_warning   = tc(tv, 'text-warning',  'yellow')
        c_error     = tc(tv, 'text-error',    'red')
        c_secondary = tc(tv, 'text-secondary','default')

        _rank_emoji = {1: '🥇', 2: '🥈', 3: '🥉'}
        ranked = [r['user'] for r in sorted(rows, key=lambda x: -x['running_gpus']) if r['running_gpus'] > 0]
        user_rank = {u: i + 1 for i, u in enumerate(ranked[:3])}

        if not rows:
            empty.update(f'No user data for the last {self.lookback_days}d')
            empty.display = True
        else:
            empty.display = False

        with self.app.batch_update():
            dt.clear()
            for r in rows:
                user      = r['user']
                run_gpus  = r['running_gpus']
                pending   = r['pending_jobs']
                total     = r['total_jobs']
                completed = r['completed']
                failed    = r['failed']
                gpu_hours = r['gpu_hours']
                avg_secs  = r['avg_secs']
                top_part  = r['top_partition']

                success_pct = (completed / total * 100) if total else 0.0
                sc = _success_color(success_pct, tv)

                emoji     = _rank_emoji.get(user_rank.get(user, 0), '')
                user_cell = Text()
                user_cell.append(emoji)
                user_cell.append(user, style=f'bold {c_primary}' if run_gpus else 'bold')

                run_cell = Text()
                if run_gpus:
                    run_cell.append(str(run_gpus), style=f'bold {c_success}')
                else:
                    run_cell.append('—', style=c_muted)

                pend_cell = Text()
                if pending:
                    pend_cell.append(str(pending), style=c_warning)
                else:
                    pend_cell.append('—', style=c_muted)

                fail_cell = Text()
                if failed:
                    fail_cell.append(str(failed), style=c_error)
                else:
                    fail_cell.append('—', style=c_muted)

                fs = (fairshares or {}).get(user)
                if fs is not None:
                    fs_color = c_success if fs >= 0.5 else c_warning if fs >= 0.2 else c_error
                    fs_cell = Text(f'{fs:.3f}', style=fs_color)
                else:
                    fs_cell = Text('—', style=c_muted)

                dt.add_row(
                    user_cell,
                    run_cell,
                    pend_cell,
                    Text(str(total) if total else '—', style=c_muted),
                    Text(_fmt_hours(gpu_hours) if gpu_hours else '—', style='bold'),
                    Text(f'{success_pct:.0f}%' if total else '—', style=sc),
                    fail_cell,
                    Text(_fmt_avg(avg_secs), style=c_muted),
                    fs_cell,
                    Text(top_part, style=c_secondary),
                    key=user,
                )

    def action_lookback_1d(self)  -> None: self.lookback_days = 1
    def action_lookback_7d(self)  -> None: self.lookback_days = 7
    def action_lookback_30d(self) -> None: self.lookback_days = 30
    def action_refresh(self)      -> None: self._load()
