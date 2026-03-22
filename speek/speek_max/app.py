"""speek+: SLURM cluster monitor TUI — built on Textual's widget infrastructure."""
from __future__ import annotations

import argparse
import getpass
import os
import signal
import subprocess
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.geometry import Size
from textual.screen import Screen
from textual.widgets import Footer, Label, TabPane, TabbedContent


from speek.speek_max.themes import DEFAULT_THEME, SPEEK_THEMES, THEME_NAMES
from speek.speek_max._utils import tc, safe
from speek.speek_max.widgets.cluster_bar import ClusterBar
from speek.speek_max.widgets.settings_widget import SettingsWidget
from speek.speek_max.widgets.confirmation import ConfirmationModal
from speek.speek_max.widgets.history_widget import HistoryWidget
from speek.speek_max.widgets.my_jobs_widget import MyJobsWidget
from speek.speek_max.widgets.node_widget import NodeWidget
from speek.speek_max.widgets.queue_widget import QueueWidget
from speek.speek_max.widgets.command_bar import CommandBar
from speek.speek_max.widgets.help_widget import HelpWidget
from speek.speek_max.widgets.stats_widget import StatsWidget
from speek.speek_max.widgets.sysinfo_widget import SysInfoWidget
from speek.speek_max.widgets.panel_divider import PanelDivider
from speek.speek_max.widgets.users_widget import UsersWidget
from speek.speek_max.widgets.logs_widget import LogsWidget


class _SpeekScreen(Screen):
    """Default screen with a corrected outer_size.

    Textual never calls _size_updated on the Screen (root widget), so
    Screen._size stays Size(0,0) forever.  Screen._on_timer_update calls
    _refresh_layout(size=None), which falls back to self.outer_size
    (= self._size = Size(0,0)) and bails out immediately – silently
    dropping every layout change that originates from an async data load
    (e.g. ClusterBar expanding after sinfo returns).

    Overriding outer_size to delegate to Screen.size (which reads
    app.size directly) makes every timer-driven layout refresh use the
    real terminal dimensions, so height:auto widgets that grow after
    mount correctly re-flow the compositor.
    """

    @property
    def outer_size(self) -> Size:
        s = self.size  # Screen.size = app.size - gutter (always correct)
        return s if s else super().outer_size


class SpeekMax(App[None]):
    """speek+: SLURM cluster monitor TUI."""

    CSS_PATH = Path(__file__).parent / "speek_max.scss"
    TITLE = "speek+"

    def get_default_screen(self) -> _SpeekScreen:
        return _SpeekScreen()

    BINDINGS = [
        Binding("1", "switch_tab('queue')",    "Queue",    show=False),
        Binding("2", "switch_tab('nodes')",    "Nodes",    show=False),
        Binding("3", "switch_tab('users')",    "Users",    show=False),
        Binding("4", "switch_tab('stats')",    "Stats",    show=False),
        Binding("5", "switch_tab('logs')",     "Logs",     show=False),
        Binding("6", "switch_tab('settings')", "Settings", show=False),
        Binding("7", "switch_tab('sysinfo')",  "Info",     show=False),
        Binding("8", "switch_tab('help')",     "Help",     show=False),
        Binding("d", "view_details",           "Details",  show=True),
        Binding("f", "switch_focus",           "⇥ Focus",  show=True),
        Binding("colon", "focus_command",      "Shell",    show=True),
        Binding("ctrl+r", "restart",             "Restart",  show=True),
        Binding("q", "quit",                   "Quit",     show=True),
    ]

    def __init__(self, theme_name: str = DEFAULT_THEME, user: str = "", **kwargs) -> None:
        super().__init__(**kwargs)
        self._initial_theme = theme_name
        self.user = user or getpass.getuser()
        self._issue_hours: int = 24
        # Refresh intervals (seconds)
        self._queue_refresh: int = 5
        self._node_refresh: int = 30
        self._history_refresh: int = 30
        self._history_lookback_days: int = 7
        # SLURM command toggles (master switches per command)
        self._cmd_squeue: bool = True
        self._cmd_scontrol: bool = True
        self._cmd_sacct: bool = True
        self._cmd_sreport: bool = True
        self._cmd_sinfo: bool = True
        # Feature flags
        self._feat_history: bool = True      # sacct history widget
        self._feat_issue_stats: bool = True  # sacct issue stats
        self._feat_priority: bool = True     # squeue priority in popup
        self._feat_sacct_details: bool = True  # sacct fallback for completed jobs
        # Highlight durations
        self._ping_duration: int = 10         # cell change ping (seconds)
        self._event_fade: int = 600           # event row fade (seconds)
        # Display
        self._time_format: str = 'relative'   # relative | absolute | both
        # Storage
        self._max_read_ids: int = 2000        # max tracked read/unread IDs
        # Cache retention (0 = forever, N = days)
        self._cache_oom_retention: int = 0
        self._cache_transition_retention: int = 0
        # Register all themes
        for name, theme in SPEEK_THEMES.items():
            try:
                self.register_theme(theme)
            except Exception:
                pass
        # Apply saved settings (overrides defaults)
        from speek.speek_max.widgets.settings_widget import SettingsWidget
        saved = SettingsWidget.load_saved_settings()
        if saved:
            for key, val in saved.items():
                if key == 'theme':
                    self._initial_theme = val
                elif hasattr(self, key):
                    setattr(self, key, val)

    def on_mount(self) -> None:
        try:
            self.theme = self._initial_theme
        except Exception:
            pass
        # Some terminals (e.g. iTerm2) don't activate mouse tracking until the
        # first SIGWINCH.  Send one to ourselves after the first render so the
        # compositor re-checks the terminal size and mouse events start working
        # immediately without requiring a manual window resize.
        self.set_timer(0.1, lambda: os.kill(os.getpid(), signal.SIGWINCH))
        from speek.speek_max.event_watcher import EventWatcher
        self._watcher = EventWatcher(self, self.user)
        self.set_timer(2.0, self._watcher.start)
        # Apply probe cache immediately (fast — just reads JSON), then
        # run a background probe on first launch or when cache is stale.
        self.run_worker(self._startup_probe, thread=True, group='probe')
        self._update_clock()
        self.set_interval(1, self._update_clock)

    def compose(self) -> ComposeResult:
        with Horizontal(id='app-title'):
            yield Label('speek+  [dim]v0.0.3[/dim]', id='app-title-left', markup=True)
            yield Label('', id='app-title-center', markup=True)
            yield Label('', id='app-title-right', markup=True)
        with Horizontal(id='main-layout'):
            with Vertical(id='left-panel'):
                yield ClusterBar()
                with TabbedContent(id='main-tabs'):
                    with TabPane("1 Queue", id="queue"):
                        yield QueueWidget()
                    with TabPane("2 Nodes", id="nodes"):
                        yield NodeWidget()
                    with TabPane("3 Users", id="users"):
                        yield UsersWidget()
                    with TabPane("4 Stats", id="stats"):
                        yield StatsWidget()
                    with TabPane("5 Logs", id="logs"):
                        yield LogsWidget()
                    with TabPane("6 Settings", id="settings"):
                        yield SettingsWidget()
                    with TabPane("7 Info", id="sysinfo"):
                        yield SysInfoWidget()
                    with TabPane("8 Help", id="help"):
                        yield HelpWidget()
            yield PanelDivider()
            with Vertical(id='side-panel'):
                yield MyJobsWidget(user=self.user)
                yield HistoryWidget()
        yield CommandBar()
        yield Footer()

    def _apply_probe_overrides(self, probe: dict) -> None:
        """Disable commands/features that the probe found unavailable."""
        from speek.speek_max.widgets.settings_widget import _CMD_ROWS, _FEAT_ROWS
        cmds = probe.get('commands', {})
        self._probe_locked: set = set()
        for _sw_id, attr, _desc in _CMD_ROWS:
            cmd_name = _sw_id.replace('cmd-', '')
            entry = cmds.get(cmd_name, {})
            available = entry.get('ok', True) if isinstance(entry, dict) else bool(entry)
            if not available:
                setattr(self, attr, False)
                self._probe_locked.add(attr)
        for _sw_id, attr, _desc, cmd_attr in _FEAT_ROWS:
            if cmd_attr in self._probe_locked:
                setattr(self, attr, False)
                self._probe_locked.add(attr)

    def _startup_probe(self) -> None:
        """Run in a worker thread: load or refresh probe cache, apply to slurm fields."""
        from speek.speek_max.probe import get_probe_results
        from speek.speek_max import slurm as _slurm
        probe = get_probe_results()   # fast if cache is fresh; runs probes if not
        _slurm.apply_probe(probe)
        self._apply_probe_overrides(probe)

        # Refresh the Info tab and apply probe locks to Settings UI
        def _refresh() -> None:
            try:
                self.query_one(SysInfoWidget)._render_results()
            except Exception:
                pass
            try:
                self.query_one(SettingsWidget)._apply_probe_locks()
            except Exception:
                pass
        self.call_from_thread(_refresh)

    def action_switch_tab(self, tab_id: str) -> None:
        try:
            self.query_one(TabbedContent).active = tab_id
        except Exception:
            pass

    def action_next_theme(self) -> None:
        idx = THEME_NAMES.index(self.theme) if self.theme in THEME_NAMES else 0
        self.theme = THEME_NAMES[(idx + 1) % len(THEME_NAMES)]

    def action_view_details(self) -> None:
        """Open details for the selected job in whichever panel has focus."""
        try:
            mj = self.query_one(MyJobsWidget)
            if mj.has_focus_within:
                mj.action_view_job()
                return
        except Exception:
            pass
        try:
            hw = self.query_one(HistoryWidget)
            if hw.has_focus_within:
                hw.action_view_log()
                return
        except Exception:
            pass

    def action_focus_command(self) -> None:
        """Focus the command bar input."""
        try:
            from textual.widgets import Input
            self.query_one(CommandBar).query_one('#cmd-input', Input).focus()
        except Exception:
            pass

    def on_command_bar_command_executed(self, event: CommandBar.CommandExecuted) -> None:
        """Forward CLI output to queue log tab and refresh data after sbatch/scancel."""
        # Append to Logs tab
        try:
            self.query_one(LogsWidget).append(
                event.command, event.output, event.success)
        except Exception:
            pass
        cmd_name = event.command.split()[0] if event.command else ''
        if cmd_name in ('sbatch', 'scancel'):
            try:
                self.query_one(MyJobsWidget)._load()
            except Exception:
                pass
            try:
                self.query_one(HistoryWidget)._load()
            except Exception:
                pass

    def on_key(self, event) -> None:
        # When History widget has focus, route 1/2/3 to its tab switching
        if event.key in ('1', '2', '3'):
            try:
                hw = self.query_one(HistoryWidget)
                if hw.has_focus_within:
                    event.prevent_default()
                    event.stop()
                    tab_map = {'1': 'tab_unread', '2': 'tab_read', '3': 'tab_all'}
                    getattr(hw, f'action_{tab_map[event.key]}')()
                    return
            except Exception:
                pass
        # When MyJobs widget has focus, route 1/2 to its tab switching
        if event.key in ('1', '2'):
            try:
                mj = self.query_one(MyJobsWidget)
                if mj.has_focus_within:
                    event.prevent_default()
                    event.stop()
                    mj_map = {'1': 'tab_current', '2': 'tab_history'}
                    getattr(mj, f'action_{mj_map[event.key]}')()
                    return
            except Exception:
                pass

        # Let CommandBar handle Tab for autocomplete when its input is focused
        if event.key == 'tab':
            try:
                cmd_input = self.query_one(CommandBar).query_one('#cmd-input')
                if cmd_input.has_focus:
                    return  # Don't intercept — CommandBar handles it
            except Exception:
                pass
            event.prevent_default()
            self.action_switch_focus()

    def action_switch_focus(self) -> None:
        """Cycle focus: left panel → MyJobs → History → left panel."""
        from speek.speek_max.widgets.datatable import SpeekDataTable
        try:
            mj = self.query_one(MyJobsWidget)
            hw = self.query_one(HistoryWidget)
            lp = self.query_one('#left-panel')
        except Exception:
            return
        if hw.has_focus_within:
            try:
                lp.query(SpeekDataTable).first().focus()
            except Exception:
                lp.focus()
        elif mj.has_focus_within:
            try:
                hw.query_one(SpeekDataTable).focus()
            except Exception:
                hw.focus()
        else:
            try:
                mj.query_one(SpeekDataTable).focus()
            except Exception:
                mj.focus()

    # ── Message handlers ──────────────────────────────────────────────────────

    def _update_clock(self) -> None:
        from datetime import datetime
        now = datetime.now()
        clock = now.strftime('%Y-%m-%d  %a  %H:%M:%S')
        try:
            self.query_one('#app-title-center', Label).update(f'[dim]{clock}[/dim]')
        except Exception:
            pass

    @safe('title update')
    def _update_title_right(self) -> None:
        tv        = self.theme_variables
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        c_error   = tc(tv, 'text-error',   'red')
        label = self.query_one('#app-title-right', Label)
        parts = []
        running   = getattr(self, '_my_running',   0)
        pending   = getattr(self, '_my_pending',   0)
        failed    = getattr(self, '_ev_failed',    0)
        timeout   = getattr(self, '_ev_timeout',   0)
        completed = getattr(self, '_ev_completed', 0)
        unread    = getattr(self, '_unread',  0)
        oom       = getattr(self, '_oom_count',    0)
        if running:
            parts.append(f'[bold {c_success}]▶ {running}[/]')
        if pending:
            parts.append(f'[bold {c_warning}]⏸ {pending}[/]')
        if oom:
            parts.append(f'[bold {c_error}]☢ {oom} OOM[/]')
        if failed:
            parts.append(f'[bold {c_error}]✗ {failed}F[/]')
        if timeout:
            parts.append(f'[bold {c_warning}]⏱ {timeout}T[/]')
        if completed:
            parts.append(f'[#4A9FD9]✔ {completed}C[/]')
        # Fallback: any other unread not covered above
        other = unread - failed - timeout - completed
        if other > 0:
            parts.append(f'[bold {c_error}]⚠ {other}[/]')
        label.update('  '.join(parts))

    def on_history_widget_unread_count(self, event: HistoryWidget.UnreadCount) -> None:
        self._unread = event.count
        self._update_title_right()

    def on_history_widget_status_counts(self, event: HistoryWidget.StatusCounts) -> None:
        self._ev_failed    = event.failed
        self._ev_timeout   = event.timeout
        self._ev_completed = event.completed
        self._update_title_right()

    def on_my_jobs_widget_running_count(self, event: MyJobsWidget.RunningCount) -> None:
        self._my_running = event.running
        self._my_pending = event.pending
        self._update_title_right()

    def on_my_jobs_widget_oom_count(self, event: MyJobsWidget.OomCount) -> None:
        self._oom_count = max(getattr(self, '_oom_count', 0), event.count)
        self._update_title_right()

    def on_history_widget_oom_count(self, event: HistoryWidget.OomCount) -> None:
        self._oom_count = max(getattr(self, '_oom_count', 0), event.count)
        self._update_title_right()

    def on_my_jobs_widget_cancel_requested(
        self, event: MyJobsWidget.CancelRequested
    ) -> None:
        job_ids = event.job_ids
        ids_str = ', '.join(job_ids)

        def _do_cancel(confirmed: bool) -> None:
            if not confirmed:
                return
            failed = []
            for jid in job_ids:
                try:
                    subprocess.run(['scancel', jid], check=True)
                except Exception as e:
                    failed.append(f'{jid}({e})')
            if failed:
                self.notify(f'scancel failed: {", ".join(failed)}', severity='error')
            else:
                self.notify(f'Cancelled: {ids_str}', severity='information')
            try:
                self.query_one(HistoryWidget)._load()
                self.query_one(MyJobsWidget)._load()
            except Exception:
                pass

        self.push_screen(
            ConfirmationModal(f'Cancel jobs: {ids_str}?'),
            _do_cancel,
        )

    # MyJobs and History handle their own JobInfoModal popups directly —
    # no app-level handlers needed.

    def action_restart(self) -> None:
        """Restart the app by re-executing the same process."""
        import sys
        self.exit()
        os.execv(sys.executable, [sys.executable] + sys.argv)


def main() -> None:
    parser = argparse.ArgumentParser(description='speek+: SLURM TUI')
    parser.add_argument('--theme', '-t', default=DEFAULT_THEME, choices=THEME_NAMES)
    parser.add_argument('--user', '-u', default=getpass.getuser())
    args = parser.parse_args()
    app = SpeekMax(theme_name=args.theme, user=args.user)
    app.run()
