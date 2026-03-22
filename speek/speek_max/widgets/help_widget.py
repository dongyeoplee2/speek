"""help_widget.py — Keyboard shortcut reference panel."""
from __future__ import annotations

from rich.table import Table
from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical, VerticalScroll
from textual.widget import Widget
from textual.widgets import Label, Static


def _kb_table(rows: list) -> Table:
    """Build a two-column table for key-binding rows."""
    t = Table(show_header=True, show_edge=False, box=None,
              pad_edge=False, padding=(0, 1, 0, 0))
    t.add_column('Key', style='bold', no_wrap=True, min_width=14, ratio=1)
    t.add_column('Action', ratio=3)
    for key, desc in rows:
        t.add_row(key, desc)
    return t


# (title, content) — content is either list of (key, desc) tuples for table,
# or a string for free-form text
_SECTIONS = [
    ('Global', [
        ('Tab / f', 'Cycle focus  Left panel → My Jobs → Events'),
        ('1', 'Switch to Queue tab'),
        ('2', 'Switch to Nodes tab'),
        ('3', 'Switch to Users tab'),
        ('4', 'Switch to Stats tab'),
        ('5', 'Switch to Settings tab'),
        ('6', 'Switch to Help tab'),
        ('q', 'Quit'),
    ]),

    ('My Jobs (right panel, top)', [
        ('z', 'Fold / unfold project group'),
        ('v', 'Expand / collapse group → individual job rows'),
        ('x', 'Cancel job(s) — shows selection then confirmation'),
        ('e', 'Explain job — opens popup on Priority tab'),
        ('l', 'Open log + scontrol popup (Output / Detail tabs)'),
        ('d', 'Open scontrol detail popup'),
        ('r', 'Refresh'),
    ]),

    ('Events / History (right panel, bottom)', [
        ('i / l', 'Open full popup (Detail / Output / GPU / Priority)'),
        ('v', 'Expand / collapse group → individual job rows'),
        ('Space', 'Toggle read / unread for selected group'),
        ('A', 'Mark all as read (unread/all tab) or all as unread (read tab)'),
        ('a', 'Show full history modal (all time)'),
        ('d', 'Set lookback to 1 day'),
        ('w', 'Set lookback to 7 days'),
        ('m', 'Set lookback to 30 days'),
        ('r', 'Refresh'),
        ('1', 'Switch to Unread tab'),
        ('2', 'Switch to Read tab'),
        ('3', 'Switch to All tab'),
    ]),

    ('Log / Detail popup', [
        ('Tab', 'Cycle between Detail, Output, GPU, and Priority tabs'),
        ('1', 'Switch to Detail tab'),
        ('2', 'Switch to Output tab'),
        ('3', 'Switch to GPU tab'),
        ('4', 'Switch to Priority tab'),
        ('g', 'Fetch live GPU stats (nvidia-smi via srun --overlap)'),
        ('r', 'Refresh details + append new log lines (incremental)'),
        ('h / l', 'Previous / next job'),
        ('Escape / q', 'Close popup'),
    ]),

    ('Queue tab', [
        ('d', 'Open scontrol detail for selected job'),
        ('r', 'Refresh'),
    ]),

    ('Nodes tab', [
        ('r', 'Refresh'),
    ]),

    ('Users tab', [
        ('d', 'Set lookback to 1 day'),
        ('w', 'Set lookback to 7 days'),
        ('m', 'Set lookback to 30 days'),
        ('r', 'Refresh'),
    ]),

    ('Stats tab [4]', [
        ('Cluster / Partition / Node / User', 'Switch breakdown dimension'),
        ('Time range dropdown', '1h / 6h / 1d / 7d / 30d / Custom'),
        ('Custom', 'Enter a custom From / To date range and click Apply'),
        ('r', 'Refresh chart data'),
    ]),

    ('Settings tab [5]', [
        ('SLURM Commands', 'Enable/disable squeue, scontrol, sacct'),
        ('Fine Controls', 'Per-feature toggles within an enabled command'),
        ('Queue/Nodes/History refresh', 'Change poll intervals (1s→5m)'),
        ('Event lookback', 'Days of history shown in events panel'),
    ]),

    ('Job State Badges', """\
Shown as bold symbols on colored backgrounds in State and E columns.

[bold white on #2d7a2d] ▶ [/] Running       [bold white on #8a7a00] ⏸ [/] Pending       [bold white on #2a6a8a] ✔ [/] Completed
[bold white on #8a2a2a] ✗ [/] Failed        [bold white on #7a4a00] ⏱ [/] Timeout       [bold white on #4a4a4a] ⊘ [/] Cancelled
[bold white on #7a2a5a] ☢ [/] OOM           [bold white on #5a2a2a] ╳ [/] Node Fail     [bold white on #5a5a2a] ⏏ [/] Preempted
[bold white on #3a3a6a] ⏯ [/] Suspended     [bold white on #2a5a5a] ↻ [/] Requeued"""),

    ('Info tab [6]', """\
Shows auto-detected SLURM cluster capabilities and probe results.
Probe runs once per day; results are cached in ~/.config/speek/system_probe.json.
[bold]Ctrl+R[/]  Re-probe (re-tests all commands and sacct fields)"""),
]


class HelpWidget(Widget):
    """Keyboard shortcut reference."""

    BORDER_TITLE = 'Help'
    can_focus = False

    def compose(self) -> ComposeResult:
        with VerticalScroll(id='help-content'):
            for title, content in _SECTIONS:
                yield Label(f'── {title} ──', classes='help-section-header')
                with Vertical(classes='help-card'):
                    if isinstance(content, list):
                        from rich.console import Console
                        from io import StringIO
                        buf = StringIO()
                        Console(file=buf, force_terminal=True, width=120).print(_kb_table(content))
                        yield Static(buf.getvalue().rstrip(), markup=False)
                    else:
                        yield Static(content, markup=True)
