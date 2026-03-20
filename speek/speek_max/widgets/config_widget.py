"""config_widget.py — Configuration panel."""
from __future__ import annotations

import getpass

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widget import Widget
from textual.widgets import Label, Static

from speek.speek_max.themes import THEME_NAMES, DEFAULT_THEME
from speek.speek_max.widgets.select import SpeekSelect


class ConfigWidget(Widget):
    """Configuration panel."""

    BORDER_TITLE = "Config"

    BINDINGS = [
        Binding('j', 'cursor_down', 'Down', show=False),
        Binding('k', 'cursor_up', 'Up', show=False),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id='config-content'):
            # ── Appearance ────────────────────────────────────────────────
            yield Label('Appearance', classes='config-section-header')
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

            # ── Session ───────────────────────────────────────────────────
            yield Label('Session', classes='config-section-header')
            with Vertical(classes='config-card'):
                with Horizontal(classes='config-row'):
                    yield Label('User', classes='config-label')
                    yield Static('', id='config-user', classes='config-value')
                with Horizontal(classes='config-row'):
                    yield Label('Queue refresh', classes='config-label')
                    yield Static('5 s', classes='config-value')
                    yield Static('running & pending jobs', classes='config-desc')
                with Horizontal(classes='config-row'):
                    yield Label('Nodes refresh', classes='config-label')
                    yield Static('30 s', classes='config-value')
                    yield Static('cluster node status', classes='config-desc')
                with Horizontal(classes='config-row'):
                    yield Label('History refresh', classes='config-label')
                    yield Static('30 s', classes='config-value')
                    yield Static('sacct job history', classes='config-desc')

            # ── About ─────────────────────────────────────────────────────
            yield Label('About', classes='config-section-header')
            with Vertical(classes='config-card'):
                with Horizontal(classes='config-row'):
                    yield Label('speek-max', classes='config-label')
                    yield Static('SLURM cluster monitor TUI', classes='config-value')
                with Horizontal(classes='config-row'):
                    yield Label('Framework', classes='config-label')
                    yield Static('Textual', classes='config-value')
                with Horizontal(classes='config-row'):
                    yield Label('Keybindings', classes='config-label')
                    yield Static('ctrl+t  next theme   q  quit   1-7  tabs', classes='config-desc')

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

    def on_select_changed(self, event: SpeekSelect.Changed) -> None:
        if event.select.id == 'theme-select' and event.value:
            self.app.theme = str(event.value)
            self._update_hint(str(event.value))

    def _update_hint(self, theme_name: str) -> None:
        kind = 'light' if theme_name == 'manuscript' else 'dark'
        self.query_one('#config-hint', Static).update(
            f'[{kind}]  ↑↓ / j k to browse, Enter to select'
        )
