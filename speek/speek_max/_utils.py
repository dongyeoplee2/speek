from __future__ import annotations
from typing import Dict

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


def tcs(tv: Dict[str, str], key: str, fallback: str = 'ansi_bright_black') -> str:
    """Safe theme color for Textual .styles.color. Maps Rich ANSI names to ansi_* prefix."""
    v = tv.get(key, fallback) or fallback
    base = v.split()[0]
    if base == 'auto':
        base = fallback
    return _RICH_TO_TEXTUAL.get(base, base)
