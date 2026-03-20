"""speek-max: SLURM cluster monitor TUI — built on Textual's widget infrastructure."""
from __future__ import annotations

import argparse
import getpass
import subprocess
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Label, TabPane, TabbedContent

from speek.speek_max.themes import DEFAULT_THEME, SPEEK_THEMES, THEME_NAMES
from speek.speek_max.widgets.cluster_bar import ClusterBar
from speek.speek_max.widgets.config_widget import ConfigWidget
from speek.speek_max.widgets.confirmation import ConfirmationModal
from speek.speek_max.widgets.history_widget import HistoryWidget
from speek.speek_max.widgets.my_jobs_widget import MyJobsWidget
from speek.speek_max.widgets.node_widget import NodeWidget
from speek.speek_max.widgets.priority_widget import PriorityWidget
from speek.speek_max.widgets.queue_widget import QueueWidget
from speek.speek_max.widgets.command_bar import CommandBar
from speek.speek_max.widgets.panel_divider import PanelDivider
from speek.speek_max.widgets.users_widget import UsersWidget


class SpeekMax(App):
    """speek-max: SLURM cluster monitor TUI."""

    CSS_PATH = Path(__file__).parent / "speek_max.scss"
    TITLE = "speek-max"

    BINDINGS = [
        Binding("1", "switch_tab('queue')",    "Queue",    show=False),
        Binding("2", "switch_tab('nodes')",    "Nodes",    show=False),
        Binding("3", "switch_tab('priority')", "Priority", show=False),
        Binding("4", "switch_tab('users')",    "Users",    show=False),
        Binding("5", "switch_tab('config')",   "Config",   show=False),
        Binding("ctrl+t", "next_theme", "Next theme", show=True),
        Binding("q", "quit", "Quit", show=True),
    ]

    def __init__(self, theme_name: str = DEFAULT_THEME, user: str = "", **kwargs) -> None:
        super().__init__(**kwargs)
        self._initial_theme = theme_name
        self.user = user or getpass.getuser()
        # Register all themes
        for name, theme in SPEEK_THEMES.items():
            try:
                self.register_theme(theme)
            except Exception:
                pass

    def on_mount(self) -> None:
        try:
            self.theme = self._initial_theme
        except Exception:
            pass
        self.set_timer(0.1, lambda: self.screen.refresh(layout=True))
        from speek.speek_max.event_watcher import EventWatcher
        self._watcher = EventWatcher(self, self.user)
        self.set_timer(2.0, self._watcher.start)

    def compose(self) -> ComposeResult:
        from rich.text import Text as RichText
        from speek.speek_max._utils import tc
        tv = {}  # theme not ready yet; use CSS class instead
        yield Label('speek-max  [dim]v0.0.3[/dim]', id='app-title', markup=True)
        with Horizontal(id='main-layout'):
            with Vertical(id='left-panel'):
                yield ClusterBar()
                with TabbedContent(id='main-tabs'):
                    with TabPane("Queue [1]", id="queue"):
                        yield QueueWidget()
                    with TabPane("Nodes [2]", id="nodes"):
                        yield NodeWidget()
                    with TabPane("Priority [3]", id="priority"):
                        yield PriorityWidget(user=self.user)
                    with TabPane("Users [4]", id="users"):
                        yield UsersWidget()
                    with TabPane("Config [5]", id="config"):
                        yield ConfigWidget()
            yield PanelDivider()
            with Vertical(id='side-panel'):
                yield MyJobsWidget(user=self.user)
                yield HistoryWidget()
        yield CommandBar()
        yield Footer()

    def action_switch_tab(self, tab_id: str) -> None:
        try:
            self.query_one(TabbedContent).active = tab_id
        except Exception:
            pass

    def action_next_theme(self) -> None:
        idx = THEME_NAMES.index(self.theme) if self.theme in THEME_NAMES else 0
        self.theme = THEME_NAMES[(idx + 1) % len(THEME_NAMES)]

    # ── Message handlers ──────────────────────────────────────────────────────

    def on_my_jobs_widget_cancel_requested(
        self, event: MyJobsWidget.CancelRequested
    ) -> None:
        job_id = event.job_id

        def _do_cancel(confirmed: bool) -> None:
            if confirmed:
                try:
                    subprocess.run(['scancel', job_id], check=True)
                    self.notify(f'Job {job_id} cancelled', severity='information')
                    try:
                        self.query_one(HistoryWidget)._load()
                        self.query_one(MyJobsWidget)._load()
                    except Exception:
                        pass
                except Exception as e:
                    self.notify(f'scancel failed: {e}', severity='error')

        self.push_screen(
            ConfirmationModal(f"Cancel job {job_id}?"),
            _do_cancel,
        )

    def on_my_jobs_widget_explain_job(
        self, event: MyJobsWidget.ExplainJob
    ) -> None:
        self.action_switch_tab('priority')
        try:
            pw = self.query_one(PriorityWidget)
            pw.load_job(event.job_id)
        except Exception:
            pass

    def on_my_jobs_widget_view_log(self, event: MyJobsWidget.ViewLog) -> None:
        self._show_log(event.job_id, event.log_path)

    def on_history_widget_view_log(self, event: HistoryWidget.ViewLog) -> None:
        from speek.speek_max.slurm import get_job_log_path

        def _fetch_and_show() -> None:
            path = get_job_log_path(event.job_id)
            self.call_from_thread(lambda: self._show_log(event.job_id, path))

        self.run_worker(_fetch_and_show, thread=True, group='log-view')

    def _show_log(self, job_id: str, log_path: str | None) -> None:
        if not log_path:
            self.notify(f'No log file found for job {job_id}', severity='warning')
            return
        try:
            from pathlib import Path as _Path
            p = _Path(log_path)
            if not p.exists():
                self.notify(f'Log file not found: {log_path}', severity='warning')
                return
            # Read last 200 lines and show as notification / modal
            lines = p.read_text(errors='replace').splitlines()
            snippet = '\n'.join(lines[-50:])
            self.notify(
                f'Log: {log_path}\n{snippet[:500]}',
                title=f'Job {job_id} log',
                severity='information',
                timeout=15,
            )
        except Exception as e:
            self.notify(f'Could not read log: {e}', severity='error')


def main() -> None:
    parser = argparse.ArgumentParser(description='speek-max: SLURM TUI')
    parser.add_argument('--theme', '-t', default=DEFAULT_THEME, choices=THEME_NAMES)
    parser.add_argument('--user', '-u', default=getpass.getuser())
    args = parser.parse_args()
    app = SpeekMax(theme_name=args.theme, user=args.user)
    app.run()
