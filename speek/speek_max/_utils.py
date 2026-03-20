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


def tcs(tv: Dict[str, str], key: str, fallback: str = 'ansi_bright_black') -> str:
    """Safe theme color for Textual .styles.color. Maps Rich ANSI names to ansi_* prefix."""
    v = tv.get(key, fallback) or fallback
    base = v.split()[0]
    if base == 'auto':
        base = fallback
    return _RICH_TO_TEXTUAL.get(base, base)
