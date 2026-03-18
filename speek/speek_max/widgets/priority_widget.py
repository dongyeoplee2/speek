"""priority_widget.py — Priority breakdown for a pending job."""
from __future__ import annotations

from typing import Dict, Optional

from rich.table import Table
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import LoadingIndicator, Static

from speek.speek_max.slurm import fetch_priority_data
from speek.speek_max._utils import tc


def _prio_bar(
    val: float,
    max_val: float = 1.0,
    width: int = 20,
    *,
    c_fill: str = 'primary',
    c_empty: str = 'bright_black',
) -> Text:
    pct = min(val / max_val, 1.0) if max_val else 0.0
    filled = int(round(pct * width))
    t = Text()
    t.append('█' * filled, style=f'bold {c_fill}')
    t.append('░' * (width - filled), style=c_empty)
    return t


def build_priority_renderable(data: Dict, tv: Dict[str, str]) -> Table:
    c_primary = tc(tv, 'primary', '#C45AFF')
    c_muted = tc(tv, 'text-muted', 'bright_black')
    c_secondary = tc(tv, 'text-secondary', 'default')
    c_warning = tc(tv, 'text-warning', '#FFD700')
    c_success = tc(tv, 'text-success', '#00FA9A')
    c_error = tc(tv, 'text-error', '#FF4500')

    table = Table(box=None, padding=(0, 1), show_header=False, expand=False)
    table.add_column('key', style=c_secondary, width=18, no_wrap=True)
    table.add_column('value', style='bold', min_width=12)
    table.add_column('bar', min_width=22)

    jid = data.get('job_id', '?')
    reason = data.get('reason', '?')
    prio = data.get('prio', {})
    share = data.get('share')
    eta = data.get('eta')

    table.add_row(Text(f'Job {jid}', style=f'bold {c_primary}'), Text(''), Text(''))
    table.add_row(Text('─' * 16, style=c_muted), Text(''), Text(''))

    table.add_row(
        Text('Pending reason'),
        Text(reason, style=f'bold {c_warning}'),
        Text(''),
    )

    if prio:
        table.add_row(Text('─' * 16, style=c_muted), Text(''), Text(''))
        table.add_row(Text('Priority breakdown', style='bold'), Text(''), Text(''))
        table.add_row(Text('─' * 16, style=c_muted), Text(''), Text(''))

        components = [
            ('AGE', 'AGE', 'Time waited'),
            ('FAIRSHARE', 'FAIRSHARE', 'Fairshare score'),
            ('JOBSIZE', 'JS', 'Job size'),
            ('QOS', 'QOS', 'QoS weight'),
        ]
        total_val = float(prio.get('PRIORITY', 0) or 0)

        for label, key, desc in components:
            val_s = prio.get(key, '0')
            try:
                val = float(val_s)
            except Exception:
                val = 0.0
            bar = _prio_bar(val, max(total_val, 1.0), c_fill=c_primary, c_empty=c_muted)
            table.add_row(
                Text(f'  {label}', style=c_secondary),
                Text(f'{val:.4f}', style='bold'),
                bar,
            )

        table.add_row(
            Text('  TOTAL', style='bold'),
            Text(f'{total_val:.4f}', style=f'bold {c_primary}'),
            Text(''),
        )

    if share is not None:
        table.add_row(Text('─' * 16, style=c_muted), Text(''), Text(''))
        share_color = c_success if share > 0.5 else c_error
        table.add_row(
            Text('Fairshare'),
            Text(f'{share:.4f}', style=f'bold {share_color}'),
            _prio_bar(share, 1.0, c_fill=c_primary, c_empty=c_muted),
        )

    if eta:
        table.add_row(Text('─' * 16, style=c_muted), Text(''), Text(''))
        table.add_row(
            Text('Predicted start'),
            Text(eta, style=f'bold {c_warning}'),
            Text(''),
        )

    return table


class PriorityWidget(Widget):
    """Priority breakdown for a pending job."""

    BORDER_TITLE = "Priority"

    BINDINGS = [
        Binding('r', 'refresh', 'Refresh', show=True),
    ]

    job_id: reactive[Optional[str]] = reactive(None)

    def __init__(self, user: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.user = user

    def compose(self) -> ComposeResult:
        yield LoadingIndicator()
        yield Static('Select a pending job and press  e  in My Jobs to explain it.',
                     id='priority-idle', classes='empty-state')
        yield Static(id='priority-content')

    def on_mount(self) -> None:
        self.query_one(LoadingIndicator).display = False
        self.query_one('#priority-idle').display = True

    def watch_job_id(self, _old: Optional[str], new: Optional[str]) -> None:
        if new:
            self._load()

    def load_job(self, job_id: str) -> None:
        self.job_id = job_id

    def _load(self) -> None:
        if not self.job_id:
            return
        jid, user = self.job_id, self.user
        self.query_one(LoadingIndicator).display = True
        self.query_one('#priority-idle').display = False
        self.run_worker(
            lambda: fetch_priority_data(jid, user),
            thread=True, exclusive=True, group='priority',
        )

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.worker.group == 'priority' and event.state == WorkerState.SUCCESS:
            self._update(event.worker.result)

    def _update(self, data: Dict) -> None:
        self.query_one(LoadingIndicator).display = False
        content = self.query_one('#priority-content', Static)
        content.update(build_priority_renderable(data, self.app.theme_variables))
        self.query_one('#priority-idle').display = False

    def action_refresh(self) -> None:
        self._load()
