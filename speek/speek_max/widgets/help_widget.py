"""help_widget.py — Keyboard shortcut reference panel."""
from __future__ import annotations

from textual.app import ComposeResult
from textual.widget import Widget
from textual.widgets import Static

_HELP_TEXT = """\
[bold $primary]── Global ─────────────────────────────────────────────[/]
  [bold]Tab / f[/]      Cycle focus  Left panel → My Jobs → Events
  [bold]1[/]            Switch to Queue tab
  [bold]2[/]            Switch to Nodes tab
  [bold]3[/]            Switch to Users tab
  [bold]4[/]            Switch to Stats tab
  [bold]5[/]            Switch to Settings tab
  [bold]6[/]            Switch to Help tab
  [bold]q[/]            Quit

[bold $primary]── My Jobs (right panel, top) ────────────────────────[/]
  [bold]z[/]            Fold / unfold project group
  [bold]v[/]            Expand / collapse group → individual job rows
  [bold]x[/]            Cancel job(s) — shows selection then confirmation
  [bold]e[/]            Explain job — opens popup on Priority tab
  [bold]l[/]            Open log + scontrol popup (Output / Detail tabs)
  [bold]d[/]            Open scontrol detail popup
  [bold]r[/]            Refresh

[bold $primary]── Events / History (right panel, bottom) ───────────[/]
  [bold]i / l[/]        Open full popup (Detail / Output / GPU / Priority)
  [bold]v[/]            Expand / collapse group → individual job rows
  [bold]Space[/]        Toggle read / unread for selected group
  [bold]A[/]            Mark all as read (unread/all tab) or all as unread (read tab)
  [bold]a[/]            Show full history modal (all time)
  [bold]d[/]            Set lookback to 1 day
  [bold]w[/]            Set lookback to 7 days
  [bold]m[/]            Set lookback to 30 days
  [bold]r[/]            Refresh
  [bold]1[/]            Switch to Unread tab
  [bold]2[/]            Switch to Read tab
  [bold]3[/]            Switch to All tab

[bold $primary]── Log / Detail popup ────────────────────────────────[/]
  [bold]Tab[/]          Cycle between Detail, Output, GPU, and Priority tabs
  [bold]1[/]            Switch to Detail tab
  [bold]2[/]            Switch to Output tab
  [bold]3[/]            Switch to GPU tab
  [bold]4[/]            Switch to Priority tab
  [bold]g[/]            Fetch live GPU stats (nvidia-smi via srun --overlap)
  [bold]r[/]            Refresh details + append new log lines (incremental)
  [bold]h / l[/]        Previous / next job
  [bold]Escape / q[/]   Close popup

[bold $primary]── Queue tab ─────────────────────────────────────────[/]
  [bold]d[/]            Open scontrol detail for selected job
  [bold]r[/]            Refresh

[bold $primary]── Nodes tab ─────────────────────────────────────────[/]
  [bold]r[/]            Refresh

[bold $primary]── Users tab ─────────────────────────────────────────[/]
  [bold]d[/]            Set lookback to 1 day
  [bold]w[/]            Set lookback to 7 days
  [bold]m[/]            Set lookback to 30 days
  [bold]r[/]            Refresh

[bold $primary]── Stats tab [4] ──────────────────────────────────────────[/]
  [bold]Cluster / Partition / Node / User[/]  Switch breakdown dimension
  [bold]Time range dropdown[/]  1h / 6h / 1d / 7d / 30d / Custom
  [bold]Custom[/]        Enter a custom From / To date range and click Apply
  [bold]r[/]             Refresh chart data

[bold $primary]── Settings tab [5] ───────────────────────────────────────[/]
  [bold]SLURM Commands[/]  Enable/disable squeue, scontrol, sacct — disables dependent features
  [bold]Fine Controls[/]   Per-feature toggles within an enabled command
  [bold]Queue/Nodes/History refresh[/]  Change poll intervals (1s→5m)
  [bold]Event lookback[/]  Days of history shown in events panel

[bold $primary]── Info tab [6] ───────────────────────────────────────────[/]
  Shows auto-detected SLURM cluster capabilities and probe results.
  Probe runs once per day; results are cached in ~/.config/speek/system_probe.json.
  [bold]Ctrl+R[/]  Re-probe (re-tests all commands and sacct fields)
"""


class HelpWidget(Widget):
    """Keyboard shortcut reference."""

    BORDER_TITLE = 'Help'
    can_focus = False

    def compose(self) -> ComposeResult:
        """Compose the help widget."""
        yield Static(_HELP_TEXT, markup=True, id='help-content')
