from __future__ import annotations
from typing import Dict

STATE_SYMBOL = {
    'RUNNING': '▶', 'PENDING': '⏸', 'COMPLETED': '✔',
    'FAILED': '✗', 'TIMEOUT': '⏱', 'CANCELLED': '⊘',
    'OUT_OF_MEMORY': '☢', 'NODE_FAIL': '╳', 'PREEMPTED': '⏏',
    'SUSPENDED': '⏯', 'REQUEUED': '↻',
}


STATE_BG = {
    'RUNNING': '#2d7a2d',   # green
    'PENDING': '#8a7a00',   # amber
    'COMPLETED': '#2a6a8a', # blue
    'FAILED': '#8a2a2a',    # red
    'TIMEOUT': '#7a4a00',   # orange
    'CANCELLED': '#4a4a4a', # gray
    'OUT_OF_MEMORY': '#7a2a5a', # magenta
    'NODE_FAIL': '#5a2a2a', # dark red
    'PREEMPTED': '#5a5a2a', # olive
    'SUSPENDED': '#3a3a6a', # indigo
    'REQUEUED': '#2a5a5a',  # teal
}


def state_sym(state: str) -> str:
    """Return the symbol for a SLURM state, handling 'CANCELLED by ...' etc."""
    base = state.split()[0] if state else ''
    return STATE_SYMBOL.get(base, base)


def state_badge(state: str) -> 'Text':
    """Return a bold symbol with colored background for a SLURM state."""
    from rich.text import Text
    base = state.split()[0] if state else ''
    sym = STATE_SYMBOL.get(base, base)
    bg = STATE_BG.get(base, '#4a4a4a')
    return Text(f' {sym} ', style=f'bold white on {bg}')

_RICH_TO_TEXTUAL = {
    'black': 'ansi_black', 'red': 'ansi_red', 'green': 'ansi_green',
    'yellow': 'ansi_yellow', 'blue': 'ansi_blue', 'magenta': 'ansi_magenta',
    'cyan': 'ansi_cyan', 'white': 'ansi_white',
    'bright_black': 'ansi_bright_black', 'bright_red': 'ansi_bright_red',
    'bright_green': 'ansi_bright_green', 'bright_yellow': 'ansi_bright_yellow',
    'bright_blue': 'ansi_bright_blue', 'bright_magenta': 'ansi_bright_magenta',
    'bright_cyan': 'ansi_bright_cyan', 'bright_white': 'ansi_bright_white',
}


def tc(tv: Dict[str, str], key: str, fallback: str = 'default') -> str:
    """Safe theme color for Rich Text styles. Strips TCSS modifiers, replaces 'auto'."""
    v = tv.get(key, fallback) or fallback
    base = v.split()[0]
    return fallback if base == 'auto' else base


def fmt_time(start_str: str, mode: str = 'relative') -> str:
    """Format a timestamp according to the display mode.

    Args:
        start_str: ISO timestamp string (e.g. '2026-03-20T10:09:16')
        mode: 'relative' (5m, 2h, 3d), 'absolute' (03/20 10:09), or 'both' (5m 10:09)
    """
    from datetime import datetime
    try:
        dt = datetime.strptime(
            start_str.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S'
        )
    except Exception:
        return start_str[:8] if start_str else '?'

    secs = (datetime.now() - dt).total_seconds()

    # Relative
    if secs < 60:
        rel = 'now'
    elif secs < 3600:
        rel = f'{max(1, int(secs / 60))}m'
    elif secs < 86400:
        rel = f'{int(secs / 3600)}h'
    else:
        rel = f'{int(secs / 86400)}d'

    # Absolute
    if secs < 86400:
        abs_t = dt.strftime('%H:%M')
    else:
        abs_t = dt.strftime('%m/%d %H:%M')

    if mode == 'absolute':
        return abs_t
    if mode == 'both':
        return f'{rel} {abs_t}'
    return rel


def report_error(widget, context: str = '') -> None:
    """Log an exception to the app's notification bar.

    Call inside an except block: ``except Exception: report_error(self, 'load')``.
    Shows the error type + message as a Textual notification so the user
    knows something went wrong instead of silently freezing.
    """
    import traceback
    import sys
    exc = sys.exc_info()[1]
    if exc is None:
        return
    short = f'{type(exc).__name__}: {exc}'
    label = f'[{context}] {short}' if context else short
    try:
        widget.app.notify(label[:120], severity='error', timeout=8)
    except Exception:
        pass
    # Also print to stderr for debugging
    try:
        traceback.print_exc(file=sys.stderr)
    except Exception:
        pass


def safe(context: str = ''):
    """Decorator: wraps a widget method so exceptions show a notification
    instead of being silently swallowed or crashing the app.

    Usage::

        @safe('history load')
        def _load(self):
            ...
    """
    import functools

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)
            except Exception:
                report_error(self, context or fn.__name__)
        return wrapper
    return decorator


def tcs(tv: Dict[str, str], key: str, fallback: str = 'ansi_bright_black') -> str:
    """Safe theme color for Textual .styles.color. Maps Rich ANSI names to ansi_* prefix."""
    v = tv.get(key, fallback) or fallback
    base = v.split()[0]
    if base == 'auto':
        base = fallback
    return _RICH_TO_TEXTUAL.get(base, base)
