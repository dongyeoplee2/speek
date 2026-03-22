"""logs_widget.py — Session-only CLI output log. Not persisted to disk."""
from __future__ import annotations

import re

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import ScrollableContainer
from textual.widget import Widget
from textual.widgets import Static

# Patterns for colorizing output
_ERROR_RE = re.compile(r'(?i)(error|failed|fatal|denied|not found|no such|traceback|exception)', re.IGNORECASE)
_WARN_RE = re.compile(r'(?i)(warning|warn|deprecated)', re.IGNORECASE)
_SUCCESS_RE = re.compile(r'(?i)(submitted|success|completed|done|ok\b)', re.IGNORECASE)
_JOB_ID_RE = re.compile(r'\b(\d{5,})\b')
_PATH_RE = re.compile(r'(/[\w./-]+)')


def _colorize_line(line: str) -> Text:
    """Colorize a single output line based on content patterns."""
    t = Text()
    if _ERROR_RE.search(line):
        t.append(line, style='red')
    elif _WARN_RE.search(line):
        t.append(line, style='yellow')
    elif _SUCCESS_RE.search(line):
        t.append(line, style='green')
    else:
        # Highlight job IDs and paths within normal lines
        pos = 0
        for m in _JOB_ID_RE.finditer(line):
            t.append(line[pos:m.start()])
            t.append(m.group(), style='bold cyan')
            pos = m.end()
        if pos == 0:
            # No job IDs found, try paths
            for m in _PATH_RE.finditer(line):
                t.append(line[pos:m.start()])
                t.append(m.group(), style='dim cyan')
                pos = m.end()
        t.append(line[pos:])
    return t


class LogsWidget(Widget):
    """Displays CLI command output from the current session. Ephemeral — not saved."""

    BORDER_TITLE = 'Logs'
    can_focus = True

    DEFAULT_CSS = """
    LogsWidget {
        background: $background;
    }
    LogsWidget ScrollableContainer {
        height: 1fr;
        padding: 0 1;
        overflow-x: auto;
        overflow-y: auto;
        background: $background;
    }
    LogsWidget ScrollableContainer:focus,
    LogsWidget ScrollableContainer:focus-within {
        background: $background;
    }
    LogsWidget Static {
        width: auto;
        background: $background;
    }
    LogsWidget Static:focus,
    LogsWidget Static:hover {
        background: $background;
    }
    .logs-output {
        color: $text-muted;
        padding: 0 0 0 2;
    }
    .logs-spacer {
        height: 1;
    }
    .logs-empty {
        color: $text-muted;
        padding: 1 2;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._count: int = 0

    def compose(self) -> ComposeResult:
        yield Static('No commands executed yet.', id='logs-empty', classes='logs-empty')
        yield ScrollableContainer(id='logs-scroll')

    def append(self, command: str, output: str, success: bool) -> None:
        """Append a CLI command + output to the log."""
        import time as _time
        self._count += 1
        ts = _time.strftime('%H:%M:%S')
        cmd_style = 'bold' if success else 'bold red'
        header = Text()
        header.append(f'[{ts}] ', style='dim')
        header.append(f'$ {command}', style=cmd_style)
        try:
            self.query_one('#logs-empty').display = False
            scroll = self.query_one('#logs-scroll', ScrollableContainer)
            scroll.mount(Static(header, markup=False))
            if output.strip():
                lines = output.strip().splitlines()
                if len(lines) > 50:
                    lines = ['...'] + lines[-50:]
                colored = Text('\n').join([_colorize_line(ln) for ln in lines])
                scroll.mount(Static(colored, markup=False,
                                    classes='logs-output'))
            scroll.mount(Static('', classes='logs-spacer'))
            scroll.scroll_end(animate=False)
        except Exception:
            pass
        self.border_title = f'Logs ({self._count})'
