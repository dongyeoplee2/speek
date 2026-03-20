"""log_modal.py — Scrollable log viewer modal with pattern highlighting."""
from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import RichLog, Static


class LogModal(ModalScreen):
    """Scrollable log tail with training/error pattern highlighting."""

    BINDINGS = [
        Binding('escape', 'dismiss', 'Close', show=True),
        Binding('q', 'dismiss', '', show=False),
    ]

    DEFAULT_CSS = """
    LogModal {
        align: center middle;
    }
    #log-modal-body {
        width: 90%;
        height: 80%;
        background: $background;
        border: round $accent;
        border-title-color: $text-accent;
        border-title-style: bold;
        padding: 0;
    }
    #log-modal-path {
        height: 1;
        padding: 0 1;
        color: $text-muted;
        background: $surface;
        text-style: italic;
    }
    #log-modal-log {
        height: 1fr;
        background: transparent;
        padding: 0 1;
    }
    """

    def __init__(self, job_id: str, log_path: str, content: Text) -> None:
        super().__init__()
        self._job_id = job_id
        self._log_path = log_path
        self._content = content

    def compose(self) -> ComposeResult:
        from textual.containers import Vertical
        with Vertical(id='log-modal-body'):
            yield Static(self._log_path, id='log-modal-path')
            yield RichLog(id='log-modal-log', highlight=False, markup=False, wrap=False)

    def on_mount(self) -> None:
        self.query_one('#log-modal-body').border_title = f'Log — job {self._job_id}'
        log = self.query_one(RichLog)
        log.write(self._content)
