"""settings_widget.py — Settings panel."""
from __future__ import annotations

import getpass
import json
from pathlib import Path

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widget import Widget
from textual.widgets import Button, Checkbox, Label, Static

from speek.speek_max.themes import THEME_NAMES, DEFAULT_THEME
from speek.speek_max.widgets.select import SpeekSelect

_CONFIG_DIR = Path.home() / '.config' / 'speek-max'
_CONFIG_FILE = _CONFIG_DIR / 'settings.json'
_STATUS_ID = '#settings-status'


_REFRESH_OPTS_FAST = [('1s', 1), ('2s', 2), ('5s', 5), ('10s', 10), ('30s', 30)]
_REFRESH_OPTS_SLOW = [('10s', 10), ('30s', 30), ('60s', 60), ('5m', 300)]

# (switch id, app attr, description of features enabled)
_CMD_ROWS = [
    ('cmd-squeue',   '_cmd_squeue',   'My Jobs panel, Queue tab, Priority popup, GPU stats'),
    ('cmd-scontrol', '_cmd_scontrol', 'Node tab status, active job detail popup'),
    ('cmd-sacct',    '_cmd_sacct',    'History, Users tab, Stats issues, completed job details'),
    ('cmd-sreport',  '_cmd_sreport',  'User stats fallback (CPU hours when sacct unavailable)'),
    ('cmd-sinfo',    '_cmd_sinfo',    'Partition list (Stats filter, job submission)'),
]

# (switch id, app attr, description, required command app attr)
_FEAT_ROWS = [
    ('feat-history',       '_feat_history',       'sacct  ·  events & history panel',         '_cmd_sacct'),
    ('feat-issue-stats',   '_feat_issue_stats',   'sacct  ·  Stats tab failure chart',         '_cmd_sacct'),
    ('feat-priority',      '_feat_priority',      'squeue  ·  Priority tab in job popup',      '_cmd_squeue'),
    ('feat-sacct-details', '_feat_sacct_details', 'sacct  ·  detail & log for completed jobs', '_cmd_sacct'),
]


class SettingsWidget(Widget):
    """Settings panel."""

    BORDER_TITLE = "Settings"

    BINDINGS = [
        Binding('j', 'cursor_down', 'Down', show=False),
        Binding('k', 'cursor_up',   'Up',   show=False),
    ]

    def compose(self) -> ComposeResult:
        """Compose the settings panel."""
        with VerticalScroll(id='config-scroll'):
            with Vertical(id='config-content'):
                # ── Appearance ────────────────────────────────────────
                yield Label('── Appearance ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    with Horizontal(classes='config-row'):
                        yield Label('Theme', classes='config-label')
                        yield SpeekSelect(
                            [(name, name) for name in THEME_NAMES],
                            value=DEFAULT_THEME,
                            id='theme-select',
                            allow_blank=False,
                        )
                    with Horizontal(classes='config-row'):
                        yield Label('', classes='config-label')
                        yield Static('', id='config-hint', classes='config-desc')

                # ── SLURM Commands ────────────────────────────────────
                yield Label('── SLURM Commands ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    yield Static(
                        'Disable a command to turn off all features that rely on it.',
                        classes='config-desc config-cmd-note',
                    )
                    for sw_id, _attr, desc in _CMD_ROWS:
                        cmd = sw_id.replace('cmd-', '')
                        with Horizontal(classes='config-row'):
                            yield Label(cmd, classes='config-label config-cmd')
                            yield Checkbox(value=True, id=sw_id, classes='config-switch')
                            yield Static(desc, classes='config-desc')

                # ── Fine Controls ─────────────────────────────────────
                yield Label('── Fine Controls ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    yield Static(
                        'Disable individual features within an enabled command.',
                        classes='config-desc config-cmd-note',
                    )
                    for sw_id, _attr, desc, _cmd_attr in _FEAT_ROWS:
                        lbl = sw_id.replace('feat-', '').replace('-', ' ').title()
                        with Horizontal(classes='config-row'):
                            yield Label(lbl, classes='config-label')
                            yield Checkbox(value=True, id=sw_id, classes='config-switch')
                            yield Static(desc, classes='config-desc')

                # ── Performance ───────────────────────────────────────
                yield Label('── Performance ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    with Horizontal(classes='config-row'):
                        yield Label('Queue refresh', classes='config-label')
                        yield SpeekSelect(
                            _REFRESH_OPTS_FAST, value=5,
                            id='queue-refresh-select', allow_blank=False,
                        )
                        yield Static('squeue — queue & my jobs', classes='config-desc')
                    with Horizontal(classes='config-row'):
                        yield Label('Nodes refresh', classes='config-label')
                        yield SpeekSelect(
                            _REFRESH_OPTS_SLOW, value=30,
                            id='node-refresh-select', allow_blank=False,
                        )
                        yield Static('scontrol — node status', classes='config-desc')
                    with Horizontal(classes='config-row'):
                        yield Label('History refresh', classes='config-label')
                        yield SpeekSelect(
                            _REFRESH_OPTS_SLOW, value=30,
                            id='history-refresh-select', allow_blank=False,
                        )
                        yield Static('sacct — job history events', classes='config-desc')
                    with Horizontal(classes='config-row'):
                        yield Label('Event lookback', classes='config-label')
                        yield SpeekSelect(
                            [('1d', 1), ('3d', 3), ('7d', 7), ('14d', 14), ('30d', 30)],
                            value=7,
                            id='history-lookback-select', allow_blank=False,
                        )
                        yield Static('days of history to show', classes='config-desc')

                # ── Stats ─────────────────────────────────────────────
                yield Label('── Stats ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    with Horizontal(classes='config-row'):
                        yield Label('Issue lookback', classes='config-label')
                        yield SpeekSelect(
                            [('1h', 1), ('6h', 6), ('12h', 12), ('24h', 24),
                             ('48h', 48), ('7d', 168)],
                            value=24,
                            id='issue-hours-select',
                            allow_blank=False,
                        )
                        yield Static('hours to scan for failed/timeout/OOM', classes='config-desc')

                # ── Display ───────────────────────────────────────────
                yield Label('── Display ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    with Horizontal(classes='config-row'):
                        yield Label('Time format', classes='config-label')
                        yield SpeekSelect(
                            [('Relative', 'relative'), ('Absolute', 'absolute'), ('Both', 'both')],
                            value='relative',
                            id='time-format-select',
                            allow_blank=False,
                        )
                        yield Static('how to show timestamps in tables', classes='config-desc')

                # ── Storage ───────────────────────────────────────────
                yield Label('── Storage ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    with Horizontal(classes='config-row'):
                        yield Label('Max read IDs', classes='config-label')
                        yield SpeekSelect(
                            [('500', 500), ('1000', 1000), ('2000', 2000), ('5000', 5000), ('10000', 10000)],
                            value=2000,
                            id='max-read-ids-select',
                            allow_blank=False,
                        )
                        yield Static('max tracked read/unread job IDs', classes='config-desc')

                # ── Highlights ────────────────────────────────────────
                yield Label('── Highlights ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    with Horizontal(classes='config-row'):
                        yield Label('Cell ping', classes='config-label')
                        yield SpeekSelect(
                            [('5s', 5), ('10s', 10), ('15s', 15), ('30s', 30), ('60s', 60)],
                            value=10,
                            id='ping-duration-select',
                            allow_blank=False,
                        )
                        yield Static('duration for change highlight on table cells', classes='config-desc')
                    with Horizontal(classes='config-row'):
                        yield Label('Event fade', classes='config-label')
                        yield SpeekSelect(
                            [('5m', 300), ('10m', 600), ('15m', 900), ('30m', 1800)],
                            value=600,
                            id='event-fade-select',
                            allow_blank=False,
                        )
                        yield Static('duration for new event row highlight', classes='config-desc')

                # ── Cache ─────────────────────────────────────────────
                yield Label('── Cache ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    yield Static('', id='cache-info-display', markup=True,
                                 classes='config-desc')
                    with Horizontal(classes='config-row'):
                        yield Label('OOM retention', classes='config-label')
                        yield SpeekSelect(
                            [('7 days', 7), ('30 days', 30), ('90 days', 90),
                             ('Forever', 0)],
                            value=0,
                            id='cache-oom-retention-select',
                            allow_blank=False,
                        )
                        yield Static('how long to keep OOM verdicts', classes='config-desc')
                    with Horizontal(classes='config-row'):
                        yield Label('Transition retention', classes='config-label')
                        yield SpeekSelect(
                            [('7 days', 7), ('30 days', 30), ('90 days', 90),
                             ('Forever', 0)],
                            value=0,
                            id='cache-transition-retention-select',
                            allow_blank=False,
                        )
                        yield Static('how long to keep job transitions', classes='config-desc')
                    with Horizontal(classes='config-row'):
                        yield Button('Clear OOM verdicts', id='cache-clear-oom-btn',
                                     classes='config-cache-btn')
                        yield Button('Clear job transitions', id='cache-clear-transitions-btn',
                                     classes='config-cache-btn')
                    with Horizontal(classes='config-row'):
                        yield Button('Clear read/unread state', id='cache-clear-read-btn',
                                     classes='config-cache-btn')
                        yield Button('Clear all caches', id='cache-clear-all-btn',
                                     classes='config-cache-btn')
                    with Horizontal(classes='config-row'):
                        yield Button('Apply cache settings', id='cache-apply-btn',
                                     classes='config-cache-btn')
                        yield Static('', id='cache-status', markup=True,
                                     classes='config-desc')

                # ── Session ───────────────────────────────────────────
                yield Label('── Session ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    with Horizontal(classes='config-row'):
                        yield Label('User', classes='config-label')
                        yield Static('', id='config-user', classes='config-value')

                # ── About ─────────────────────────────────────────────
                yield Label('── About ──', classes='config-section-header')
                with Vertical(classes='config-card'):
                    with Horizontal(classes='config-row'):
                        yield Label('speek-max', classes='config-label')
                        yield Static('SLURM cluster monitor TUI', classes='config-value')
                    with Horizontal(classes='config-row'):
                        yield Label('Framework', classes='config-label')
                        yield Static('Textual', classes='config-value')
                    with Horizontal(classes='config-row'):
                        yield Label('Keybindings', classes='config-label')
                        yield Static('ctrl+t  next theme   q  quit   1-6  tabs', classes='config-desc')

                # ── Save / Reset ──────────────────────────────────────
                with Horizontal(id='settings-actions'):
                    yield Button('💾 Save', id='settings-save-btn')
                    yield Button('↺ Reset', id='settings-reset-btn')
                    yield Static('', id='settings-status', markup=True)

    def on_mount(self) -> None:
        current = self.app.theme
        try:
            self.query_one('#theme-select', SpeekSelect).value = current
        except Exception:
            pass
        self._update_hint(current)
        try:
            self.query_one('#config-user', Static).update(getpass.getuser())
        except Exception:
            pass
        # Sync selects
        for sel_id, attr in [
            ('#queue-refresh-select',    '_queue_refresh'),
            ('#node-refresh-select',     '_node_refresh'),
            ('#history-refresh-select',  '_history_refresh'),
            ('#history-lookback-select', '_history_lookback_days'),
            ('#issue-hours-select',      '_issue_hours'),
            ('#ping-duration-select',    '_ping_duration'),
            ('#event-fade-select',       '_event_fade'),
            ('#max-read-ids-select',     '_max_read_ids'),
            ('#cache-oom-retention-select',       '_cache_oom_retention'),
            ('#cache-transition-retention-select', '_cache_transition_retention'),
        ]:
            val = getattr(self.app, attr, None)
            if val is not None:
                try:
                    self.query_one(sel_id, SpeekSelect).value = val
                except Exception:
                    pass
        # Sync command switches
        for sw_id, attr, _desc in _CMD_ROWS:
            try:
                self.query_one(f'#{sw_id}', Checkbox).value = getattr(self.app, attr, True)
            except Exception:
                pass
        # Sync feature switches
        for sw_id, attr, _desc, _cmd in _FEAT_ROWS:
            try:
                self.query_one(f'#{sw_id}', Checkbox).value = getattr(self.app, attr, True)
            except Exception:
                pass

    def on_select_changed(self, event: SpeekSelect.Changed) -> None:
        sel_id = event.select.id
        val = event.value
        if val is None:
            return
        if sel_id == 'theme-select':
            self.app.theme = str(val)
            self._update_hint(str(val))
        elif sel_id == 'issue-hours-select':
            self.app._issue_hours = int(val)
        elif sel_id == 'time-format-select':
            self.app._time_format = str(val)
        elif sel_id == 'ping-duration-select':
            self.app._ping_duration = int(val)
        elif sel_id == 'event-fade-select':
            self.app._event_fade = int(val)
        elif sel_id == 'max-read-ids-select':
            self.app._max_read_ids = int(val)
        elif sel_id in ('queue-refresh-select', 'node-refresh-select', 'history-refresh-select'):
            target = sel_id.replace('-refresh-select', '')
            self._apply_refresh(target, int(val))
        elif sel_id == 'history-lookback-select':
            self._apply_lookback(int(val))

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        sw_id = event.checkbox.id
        cmd_map = {row[0]: row[1] for row in _CMD_ROWS}
        if sw_id in cmd_map:
            setattr(self.app, cmd_map[sw_id], event.value)
            return
        feat_map = {row[0]: row[1] for row in _FEAT_ROWS}
        if sw_id in feat_map:
            setattr(self.app, feat_map[sw_id], event.value)

    def _apply_refresh(self, target: str, seconds: int) -> None:
        _attr = {'queue': '_queue_refresh', 'node': '_node_refresh', 'history': '_history_refresh'}
        if target in _attr:
            setattr(self.app, _attr[target], seconds)
        from speek.speek_max.widgets.my_jobs_widget import MyJobsWidget
        from speek.speek_max.widgets.queue_widget import QueueWidget
        from speek.speek_max.widgets.node_widget import NodeWidget
        from speek.speek_max.widgets.history_widget import HistoryWidget
        targets = {
            'queue':   [MyJobsWidget, QueueWidget],
            'node':    [NodeWidget],
            'history': [HistoryWidget],
        }
        for cls in targets.get(target, []):
            try:
                self.app.query_one(cls).set_refresh_interval(seconds)
            except Exception:
                pass

    def _apply_lookback(self, days: int) -> None:
        self.app._history_lookback_days = days
        from speek.speek_max.widgets.history_widget import HistoryWidget
        try:
            self.app.query_one(HistoryWidget).set_lookback(days)
        except Exception:
            pass

    def _lock_checkbox(self, sw_id: str, message: str) -> None:
        """Disable a checkbox and replace its description with a message."""
        try:
            cb = self.query_one(f'#{sw_id}', Checkbox)
            cb.value = False
            cb.disabled = True
            row = cb.parent
            if row is None:
                return
            for child in row.children:
                if isinstance(child, Static) and 'config-desc' in child.classes:
                    child.update(message)
                    break
        except Exception:
            pass

    def _apply_probe_locks(self) -> None:
        """Disable checkboxes for commands/features the probe found unavailable."""
        locked = getattr(self.app, '_probe_locked', set())
        if not locked:
            return
        for sw_id, attr, _desc in _CMD_ROWS:
            if attr in locked:
                self._lock_checkbox(sw_id, '[dim](unavailable on this cluster)[/dim]')
        for sw_id, attr, _desc, _cmd_attr in _FEAT_ROWS:
            if attr in locked:
                self._lock_checkbox(sw_id, '[dim](unavailable — requires disabled command)[/dim]')

    def _update_hint(self, theme_name: str) -> None:
        kind = 'light' if theme_name == 'manuscript' else 'dark'
        self.query_one('#config-hint', Static).update(
            f'[{kind}]  ↑↓ / j k to browse, Enter to select'
        )

    # ── Save / Reset ──────────────────────────────────────────────────────────

    # Default values — must match SpeekMax.__init__
    _DEFAULTS = {
        'theme':                DEFAULT_THEME,
        '_cmd_squeue':          True,
        '_cmd_scontrol':        True,
        '_cmd_sacct':           True,
        '_cmd_sreport':         True,
        '_cmd_sinfo':           True,
        '_feat_history':        True,
        '_feat_issue_stats':    True,
        '_feat_priority':       True,
        '_feat_sacct_details':  True,
        '_queue_refresh':       5,
        '_node_refresh':        30,
        '_history_refresh':     30,
        '_history_lookback_days': 7,
        '_issue_hours':         24,
        '_ping_duration':       10,
        '_event_fade':          600,
        '_time_format':         'relative',
        '_max_read_ids':        2000,
        '_cache_oom_retention':       0,
        '_cache_transition_retention': 0,
    }

    def _gather_settings(self) -> dict:
        """Collect current settings from the app."""
        app = self.app
        return {
            'theme':                app.theme,
            '_cmd_squeue':          app._cmd_squeue,
            '_cmd_scontrol':        app._cmd_scontrol,
            '_cmd_sacct':           app._cmd_sacct,
            '_cmd_sreport':         app._cmd_sreport,
            '_cmd_sinfo':           app._cmd_sinfo,
            '_feat_history':        app._feat_history,
            '_feat_issue_stats':    app._feat_issue_stats,
            '_feat_priority':       app._feat_priority,
            '_feat_sacct_details':  app._feat_sacct_details,
            '_queue_refresh':       app._queue_refresh,
            '_node_refresh':        app._node_refresh,
            '_history_refresh':     app._history_refresh,
            '_history_lookback_days': app._history_lookback_days,
            '_issue_hours':         app._issue_hours,
            '_cache_oom_retention':       getattr(app, '_cache_oom_retention', 0),
            '_cache_transition_retention': getattr(app, '_cache_transition_retention', 0),
        }

    def _apply_settings(self, settings: dict) -> None:
        """Apply a settings dict to the app and sync the UI widgets."""
        app = self.app
        # Theme
        theme = settings.get('theme', self._DEFAULTS['theme'])
        if theme in THEME_NAMES:
            app.theme = theme
            try:
                self.query_one('#theme-select', SpeekSelect).value = theme
            except Exception:
                pass
            self._update_hint(theme)
        # Bool and int attrs
        for key, default in self._DEFAULTS.items():
            if key == 'theme':
                continue
            val = settings.get(key, default)
            setattr(app, key, val)
        # Sync Switch widgets
        for sw_id, attr, _desc in _CMD_ROWS:
            try:
                self.query_one(f'#{sw_id}', Checkbox).value = getattr(app, attr, True)
            except Exception:
                pass
        for sw_id, attr, _desc, _cmd in _FEAT_ROWS:
            try:
                self.query_one(f'#{sw_id}', Checkbox).value = getattr(app, attr, True)
            except Exception:
                pass
        # Sync Select widgets
        for sel_id, attr in [
            ('#queue-refresh-select',    '_queue_refresh'),
            ('#node-refresh-select',     '_node_refresh'),
            ('#history-refresh-select',  '_history_refresh'),
            ('#history-lookback-select', '_history_lookback_days'),
            ('#issue-hours-select',      '_issue_hours'),
            ('#ping-duration-select',    '_ping_duration'),
            ('#event-fade-select',       '_event_fade'),
            ('#max-read-ids-select',     '_max_read_ids'),
            ('#cache-oom-retention-select',       '_cache_oom_retention'),
            ('#cache-transition-retention-select', '_cache_transition_retention'),
        ]:
            try:
                self.query_one(sel_id, SpeekSelect).value = getattr(app, attr)
            except Exception:
                pass

    def _save_settings(self) -> None:
        """Save current settings to ~/.config/speek-max/settings.json."""
        try:
            _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            _CONFIG_FILE.write_text(json.dumps(self._gather_settings(), indent=2))
            self.query_one(_STATUS_ID, Static).update(
                f'[bold green]Saved[/bold green] [dim]{_CONFIG_FILE}[/dim]')
        except Exception as e:
            self.query_one(_STATUS_ID, Static).update(
                f'[bold red]Error:[/bold red] {e}')

    def _reset_settings(self) -> None:
        """Reset all settings to defaults and remove config file."""
        self._apply_settings(dict(self._DEFAULTS))
        try:
            _CONFIG_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            self.query_one(_STATUS_ID, Static).update(
                '[bold yellow]Reset to defaults[/bold yellow]')
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id
        if bid == 'settings-save-btn':
            self._save_settings()
        elif bid == 'settings-reset-btn':
            from speek.speek_max.widgets.confirmation import ConfirmationModal

            def _on_confirm(confirmed: bool) -> None:
                if confirmed:
                    self._reset_settings()

            self.app.push_screen(
                ConfirmationModal('Reset all settings to defaults?'),
                _on_confirm,
            )
        elif bid == 'cache-clear-oom-btn':
            self._confirm_clear_cache('OOM verdicts', self._do_clear_oom)
        elif bid == 'cache-clear-transitions-btn':
            self._confirm_clear_cache('job transitions', self._do_clear_transitions)
        elif bid == 'cache-clear-read-btn':
            self._confirm_clear_cache('read/unread state', self._do_clear_read)
        elif bid == 'cache-clear-all-btn':
            self._confirm_clear_cache('ALL caches (in-memory + disk)', self._do_clear_all)
        elif bid == 'cache-apply-btn':
            self._apply_cache_settings()

    # ── Cache management ────────────────────────────────────────────────────

    _CACHE_DIR = Path.home() / '.cache' / 'speek'

    def _confirm_clear_cache(self, label: str, action) -> None:
        from speek.speek_max.widgets.confirmation import ConfirmationModal

        def _on_confirm(confirmed: bool) -> None:
            if confirmed:
                action()

        self.app.push_screen(
            ConfirmationModal(f'Clear {label}?'),
            _on_confirm,
        )

    def _do_clear_oom(self) -> None:
        try:
            f = self._CACHE_DIR / 'oom_verdicts.json'
            if f.exists():
                f.unlink()
            # Also clear in-memory cache
            from speek.speek_max.widgets.history_widget import HistoryWidget
            HistoryWidget._oom_scan_cache.clear()
            HistoryWidget._oom_disk_loaded = False
        except Exception:
            pass
        self.app.notify('OOM verdicts cleared', title='Cache')
        self._refresh_cache_info()

    def _do_clear_transitions(self) -> None:
        try:
            f = self._CACHE_DIR / 'job_transitions.json'
            if f.exists():
                f.unlink()
        except Exception:
            pass
        self.app.notify('Job transitions cleared', title='Cache')
        self._refresh_cache_info()

    def _do_clear_read(self) -> None:
        try:
            f = self._CACHE_DIR / 'history_read.json'
            if f.exists():
                f.unlink()
        except Exception:
            pass
        self.app.notify('Read/unread state cleared', title='Cache')
        self._refresh_cache_info()

    def _do_clear_all(self) -> None:
        # Clear in-memory caches
        try:
            from speek.speek_max.slurm import clear_all_caches
            clear_all_caches()
        except Exception:
            pass
        # Clear disk caches
        self._do_clear_oom()
        self._do_clear_transitions()
        self._do_clear_read()
        self.app.notify('All caches cleared', title='Cache')
        self._refresh_cache_info()

    def _apply_cache_settings(self) -> None:
        """Apply cache retention settings and save to config."""
        try:
            oom_ret = self.query_one('#cache-oom-retention-select', SpeekSelect).value
            trans_ret = self.query_one('#cache-transition-retention-select', SpeekSelect).value
            self.app._cache_oom_retention = int(oom_ret) if oom_ret is not None else 0
            self.app._cache_transition_retention = int(trans_ret) if trans_ret is not None else 0
            self._save_settings()
            self.query_one('#cache-status', Static).update(
                '[bold green]Cache settings applied[/bold green]')
        except Exception as e:
            try:
                self.query_one('#cache-status', Static).update(
                    f'[bold red]Error: {e}[/bold red]')
            except Exception:
                pass

    def _refresh_cache_info(self) -> None:
        """Update the cache info display in the settings panel."""
        try:
            self._update_cache_info_display()
        except Exception:
            pass

    def _update_cache_info_display(self) -> None:
        """Render current cache sizes into the info display widget."""
        lines = []
        # In-memory cache stats
        try:
            from speek.speek_max.slurm import get_cache_stats
            stats = get_cache_stats()
            total_entries = sum(stats.values())
            lines.append(f'[dim]In-memory:[/dim] {total_entries} entries across {len(stats)} caches')
        except Exception:
            lines.append('[dim]In-memory:[/dim] unavailable')

        # Disk cache sizes
        cache_files = [
            ('oom_verdicts.json', 'OOM verdicts'),
            ('job_transitions.json', 'Job transitions'),
            ('history_read.json', 'Read/unread state'),
        ]
        for fname, label in cache_files:
            p = self._CACHE_DIR / fname
            if p.exists():
                try:
                    size = p.stat().st_size
                    data = json.loads(p.read_text())
                    n = len(data) if isinstance(data, dict) else (
                        len(data.get('read_ids', [])) if isinstance(data, dict) else 0)
                    if isinstance(data, dict) and 'read_ids' in data:
                        n = len(data['read_ids'])
                    elif isinstance(data, dict):
                        n = len(data)
                    sz = f'{size / 1024:.1f} KB' if size >= 1024 else f'{size} B'
                    lines.append(f'[dim]{label}:[/dim] {n} entries, {sz}')
                except Exception:
                    lines.append(f'[dim]{label}:[/dim] exists (unreadable)')
            else:
                lines.append(f'[dim]{label}:[/dim] empty')

        # Last cleared timestamp
        try:
            from speek.speek_max.slurm import get_last_cache_clear
            import time as _time
            ts = get_last_cache_clear()
            if ts is not None:
                ago = _time.monotonic() - ts
                if ago < 60:
                    lines.append(f'[dim]Last cleared:[/dim] {int(ago)}s ago')
                elif ago < 3600:
                    lines.append(f'[dim]Last cleared:[/dim] {int(ago / 60)}m ago')
                else:
                    lines.append(f'[dim]Last cleared:[/dim] {int(ago / 3600)}h ago')
        except Exception:
            pass

        try:
            self.query_one('#cache-info-display', Static).update('\n'.join(lines))
        except Exception:
            pass

    def on_show(self) -> None:
        """Refresh cache info display when the settings panel becomes visible."""
        self._refresh_cache_info()

    @staticmethod
    def load_saved_settings() -> dict:
        """Load settings from disk. Returns empty dict if no file."""
        try:
            if _CONFIG_FILE.exists():
                return json.loads(_CONFIG_FILE.read_text())
        except Exception:
            pass
        return {}
