"""node_widget.py — Per-node GPU status panel."""
from __future__ import annotations

from typing import List, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.widget import Widget
from textual.widgets import LoadingIndicator, Static

from speek.speek_max.slurm import parse_nodes
from speek.speek_max._utils import tc
from speek.speek_max.widgets.datatable import SpeekDataTable


class NodeWidget(Widget):
    """Per-node GPU status."""

    BORDER_TITLE = "Nodes"
    can_focus = True

    BINDINGS = [
        Binding('r', 'refresh', 'Refresh', show=True),
    ]

    def compose(self) -> ComposeResult:
        """Compose the node status widget."""
        yield LoadingIndicator()
        yield Static('', id='node-empty', classes='empty-state')
        yield SpeekDataTable(id='node-dt', cursor_type='row')

    def on_mount(self) -> None:
        dt = self.query_one(SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('Node',      width=10)
        dt.add_column('Partition', width=6)
        dt.add_column('GPU',       width=8)
        dt.add_column('Free',      width=4)
        dt.add_column('Total',     width=4)
        dt.add_column('State',     width=7)
        dt.add_column('Reason',    width=12)

    def on_show(self) -> None:
        self._load()
        if not hasattr(self, '_interval_started'):
            self._interval_started = True
            interval = getattr(self.app, '_node_refresh', 30)
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
        if not getattr(self.app, '_cmd_scontrol', True):
            return
        self.run_worker(self._fetch, thread=True, exclusive=True, group='nodes')

    def _fetch(self) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        rows = parse_nodes()
        if not worker.is_cancelled:
            self.app.call_from_thread(self._update, rows)

    def _update(self, rows: List[Tuple]) -> None:
        self.query_one(LoadingIndicator).display = False
        empty = self.query_one('#node-empty', Static)
        dt = self.query_one(SpeekDataTable)
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_secondary = tc(tv, 'text-secondary', 'default')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        c_error = tc(tv, 'text-error', 'red')
        state_color = {
            'idle': c_success,
            'mixed': c_warning,
            'alloc': c_error,
            'drain': c_error,
            'down': c_error,
            'maint': c_muted,
        }
        if not rows:
            empty.update('No nodes reported by SLURM')
            empty.display = True
        else:
            empty.display = False

        with self.app.batch_update():
            dt.clear()
            for r in rows:
                node, parts, model, free, total, state, reason = r
                sc = state_color.get(state, 'default')
                fc = c_success if free > 0 else c_error
                dt.add_row(
                    Text(node, style='bold'),
                    Text(parts, style=c_secondary),
                    Text(model),
                    Text(str(free), style=f'bold {fc}'),
                    Text(str(total), style=c_muted),
                    Text(state, style=f'bold {sc}'),
                    Text(reason, style=c_muted),
                    key=node,
                )

    def action_refresh(self) -> None:
        self._load()
