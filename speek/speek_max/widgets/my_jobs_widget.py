"""my_jobs_widget.py — Current user's jobs panel."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.message import Message
from textual.widget import Widget
from textual.widgets import LoadingIndicator, Static

from speek.speek_max.slurm import fetch_all_priorities, fetch_job_details, fetch_my_jobs, get_job_log_path
from speek.speek_max._utils import tc
from speek.speek_max.widgets.datatable import SpeekDataTable


class MyJobsWidget(Widget):
    """Current user's running + pending jobs. x=cancel, e=explain, s=log."""

    BORDER_TITLE = "My Jobs"
    can_focus = True

    BINDINGS = [
        Binding('x', 'cancel_job',  'Cancel',  show=True),
        Binding('e', 'explain_job', 'Explain', show=True),
        Binding('d', 'job_detail',  'Detail',  show=True),
        Binding('l', 'view_log',    'Log',     show=True),
        Binding('r', 'refresh',     'Refresh', show=True),
    ]

    # ── Messages ──────────────────────────────────────────────────────────────

    class ExplainJob(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    class ViewLog(Message):
        def __init__(self, job_id: str, log_path: Optional[str]) -> None:
            super().__init__()
            self.job_id = job_id
            self.log_path = log_path

    class CancelRequested(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def __init__(self, user: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.user = user

    def compose(self) -> ComposeResult:
        yield LoadingIndicator()
        yield Static('', id='myjobs-empty', classes='empty-state')
        yield SpeekDataTable(id='myjobs-dt', cursor_type='row', show_cursor=True)

    def on_mount(self) -> None:
        dt = self.query_one(SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('JobID',     width=9)
        dt.add_column('Name',      width=20)
        dt.add_column('Partition', width=10)
        dt.add_column('GPU',       width=5)
        dt.add_column('State',     width=9)
        dt.add_column('Elapsed',   width=9)
        dt.add_column('ETA',       width=10)
        dt.add_column('Rank',      width=7)
        self._load()
        self.set_interval(5, self._load)

    def on_click(self, event) -> None:
        try:
            self.query_one(SpeekDataTable).focus()
        except Exception:
            pass

    # ── Data ──────────────────────────────────────────────────────────────────

    def _load(self) -> None:
        self.run_worker(self._fetch, thread=True, exclusive=True, group='myjobs')

    def _fetch(self) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        rows = fetch_my_jobs(self.user)
        priorities = fetch_all_priorities()
        if not worker.is_cancelled:
            self.app.call_from_thread(self._update, rows, priorities)

    def _update(self, rows: List[Tuple], priorities: Dict = None) -> None:
        self.query_one(LoadingIndicator).display = False
        empty = self.query_one('#myjobs-empty', Static)
        dt = self.query_one(SpeekDataTable)
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_secondary = tc(tv, 'text-secondary', 'default')
        c_success = tc(tv, 'text-success', '#00FA9A')
        c_warning = tc(tv, 'text-warning', '#FFD700')
        c_error = tc(tv, 'text-error', '#FF4500')
        state_style = {
            'RUNNING': f'bold {c_success}',
            'PENDING': c_warning,
            'FAILED': f'bold {c_error}',
        }
        if not rows:
            empty.update(f'No active or pending jobs for {self.user}')
            empty.display = True
        else:
            empty.display = False

        # Build partition rank maps from priorities: {partition: [(jid, total), ...] sorted desc}
        part_ranked: Dict[str, list] = {}
        for jid_p, info in (priorities or {}).items():
            p = info.get('partition', '')
            part_ranked.setdefault(p, []).append((jid_p, info.get('total', 0)))
        for p in part_ranked:
            part_ranked[p].sort(key=lambda x: -x[1])
        part_rank_index: Dict[str, Dict[str, int]] = {}
        for p, lst in part_ranked.items():
            part_rank_index[p] = {jid_p: i + 1 for i, (jid_p, _) in enumerate(lst)}

        with self.app.batch_update():
            dt.clear()
            for r in rows:
                jid, name, part, gpus, state, elapsed, eta = r
                if state == 'PENDING' and priorities:
                    rank_pos = part_rank_index.get(part, {}).get(jid)
                    total_pending = len(part_ranked.get(part, []))
                    if rank_pos is not None:
                        rank_cell = Text(f'#{rank_pos}/{total_pending}', style=c_warning)
                    else:
                        rank_cell = Text('—', style=c_muted)
                else:
                    rank_cell = Text('—', style=c_muted)
                dt.add_row(
                    Text(jid, style=c_muted),
                    Text(name, style='bold'),
                    Text(part, style=c_secondary),
                    Text(gpus, style='bold'),
                    Text(state, style=state_style.get(state, f'dim {c_muted}')),
                    Text(elapsed, style=c_muted),
                    Text(eta, style=f'{c_warning} italic') if eta else Text(''),
                    rank_cell,
                    key=jid,
                )

    # ── Actions ───────────────────────────────────────────────────────────────

    def _selected_job_id(self) -> Optional[str]:
        dt = self.query_one(SpeekDataTable)
        if dt.row_count == 0:
            return None
        try:
            row = dt.get_row_at(dt.cursor_row)
            return str(row[0])
        except Exception:
            return None

    def action_refresh(self) -> None:
        self._load()

    def action_explain_job(self) -> None:
        jid = self._selected_job_id()
        if jid:
            self.post_message(self.ExplainJob(jid))

    def action_view_log(self) -> None:
        jid = self._selected_job_id()
        if not jid:
            return

        def _get_log() -> None:
            from speek.speek_max.log_scan import scan_log
            path = get_job_log_path(jid)
            if not path:
                self.app.call_from_thread(
                    lambda: self.app.notify(f'No log file found for job {jid}', severity='warning')
                )
                return
            content = scan_log(path)
            if content is None:
                self.app.call_from_thread(
                    lambda: self.app.notify(f'Log not found: {path}', severity='warning')
                )
                return
            self.app.call_from_thread(lambda: self._open_log_modal(jid, path, content))

        self.run_worker(_get_log, thread=True, group='log-view')

    def _open_log_modal(self, jid: str, path: str, content) -> None:
        from speek.speek_max.widgets.log_modal import LogModal
        self.app.push_screen(LogModal(jid, path, content))

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

    def action_cancel_job(self) -> None:
        jid = self._selected_job_id()
        if jid:
            self.post_message(self.CancelRequested(jid))
