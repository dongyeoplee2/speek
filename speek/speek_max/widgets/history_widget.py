"""history_widget.py — sacct job history panel."""
from __future__ import annotations

from typing import List, Optional, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Button, Label, LoadingIndicator, Static

from speek.speek_max.slurm import fetch_history, fetch_job_details
from speek.speek_max._utils import tc, tcs
from speek.speek_max.widgets.datatable import SpeekDataTable


class HistoryWidget(Widget):
    """sacct job history. d/w/m toggles lookback window."""

    BORDER_TITLE = "History"
    can_focus = True

    BINDINGS = [
        Binding('i', 'job_detail', 'Detail', show=True),
        Binding('d', 'lookback_1d', '1d', show=True),
        Binding('w', 'lookback_7d', '7d', show=True),
        Binding('m', 'lookback_30d', '30d', show=True),
        Binding('l', 'view_log', 'Log', show=True),
        Binding('r', 'refresh', 'Refresh', show=True),
    ]

    lookback_days: reactive[int] = reactive(7)

    class ViewLog(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    def compose(self) -> ComposeResult:
        with Horizontal(id='history-toolbar'):
            yield Label('Lookback:', id='lb-label')
            yield Label('[ 1d ]', id='lb-1d')
            yield Label('[ 7d ]', id='lb-7d')
            yield Label('[30d]', id='lb-30d')
            yield Button('Refresh', id='collect-btn', variant='default')
        yield LoadingIndicator()
        yield Static('', id='history-empty', classes='empty-state')
        yield SpeekDataTable(id='history-dt', cursor_type='row', show_cursor=True)

    def on_mount(self) -> None:
        dt = self.query_one(SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('JobID',     width=9)
        dt.add_column('Name',      width=20)
        dt.add_column('Partition', width=10)
        dt.add_column('Start',     width=16)
        dt.add_column('Elapsed',   width=9)
        dt.add_column('State',     width=12)
        dt.add_column('Exit',      width=6)
        self._update_toolbar()
        self._load()
        self.set_interval(30, self._load)

    def watch_lookback_days(self, _old: int, _new: int) -> None:
        self._update_toolbar()
        self._load()

    def _update_toolbar(self) -> None:
        tv = self.app.theme_variables
        active = tcs(tv, 'primary', '#C45AFF')
        inactive = tcs(tv, 'text-muted', 'ansi_bright_black')
        for days, wid in [(1, '#lb-1d'), (7, '#lb-7d'), (30, '#lb-30d')]:
            label = self.query_one(wid, Label)
            if self.lookback_days == days:
                label.styles.color = active
                label.styles.text_style = 'bold'
            else:
                label.styles.color = inactive
                label.styles.text_style = 'none'

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'collect-btn':
            self._load()

    def on_click(self, event) -> None:
        try:
            self.query_one(SpeekDataTable).focus()
        except Exception:
            pass

    def _load(self) -> None:
        days = self.lookback_days
        self.run_worker(
            lambda: fetch_history(days),
            thread=True, exclusive=True, group='history',
        )

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.worker.group == 'history' and event.state == WorkerState.SUCCESS:
            self._update(event.worker.result)

    def _update(self, rows: List[Tuple]) -> None:
        self.query_one(LoadingIndicator).display = False
        empty = self.query_one('#history-empty', Static)
        dt = self.query_one(SpeekDataTable)
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_secondary = tc(tv, 'text-secondary', 'default')
        c_error = tc(tv, 'text-error', '#FF4500')
        c_success = tc(tv, 'text-success', '#00FA9A')
        c_warning = tc(tv, 'text-warning', '#FFD700')
        state_style = {
            'COMPLETED': f'dim {c_muted}',
            'FAILED': f'bold {c_error}',
            'TIMEOUT': f'bold {c_error}',
            'CANCELLED': f'dim {c_error}',
            'OUT_OF_MEMORY': f'bold {c_error}',
            'RUNNING': f'bold {c_success}',
            'PENDING': c_warning,
        }
        if not rows:
            empty.update(f'No jobs found in the last {self.lookback_days}d')
            empty.display = True
        else:
            empty.display = False

        with self.app.batch_update():
            dt.clear()
            for r in rows:
                jid, name, part, start, elapsed, state, exit_code = r
                exit_color = c_muted if exit_code == '0:0' else c_error
                dt.add_row(
                    Text(jid, style=c_muted),
                    Text(name, style='bold'),
                    Text(part, style=c_secondary),
                    Text(start, style=c_muted),
                    Text(elapsed, style=c_muted),
                    Text(state, style=state_style.get(state, 'default')),
                    Text(exit_code, style=exit_color),
                    key=jid,
                )

    def _selected_job_id(self) -> Optional[str]:
        dt = self.query_one(SpeekDataTable)
        if dt.row_count == 0:
            return None
        try:
            row = dt.get_row_at(dt.cursor_row)
            return str(row[0])
        except Exception:
            return None

    def action_lookback_1d(self) -> None:
        self.lookback_days = 1

    def action_lookback_7d(self) -> None:
        self.lookback_days = 7

    def action_lookback_30d(self) -> None:
        self.lookback_days = 30

    def action_refresh(self) -> None:
        self._load()

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

    def action_view_log(self) -> None:
        jid = self._selected_job_id()
        if not jid:
            return

        def _fetch_and_show() -> None:
            from speek.speek_max.slurm import get_job_log_path
            from speek.speek_max.log_scan import scan_log
            path = get_job_log_path(jid)
            if not path:
                self.app.call_from_thread(
                    lambda: self.app.notify(f'No log file for job {jid}', severity='warning')
                )
                return
            content = scan_log(path)
            if content is None:
                self.app.call_from_thread(
                    lambda: self.app.notify(f'Log not found: {path}', severity='warning')
                )
                return
            from speek.speek_max.widgets.log_modal import LogModal
            self.app.call_from_thread(
                lambda: self.app.push_screen(LogModal(jid, path, content))
            )

        self.run_worker(_fetch_and_show, thread=True, group='log-view')
