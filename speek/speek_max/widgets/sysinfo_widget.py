"""sysinfo_widget.py — System Info tab: SLURM capability probe results."""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import List, Tuple

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.widget import Widget
from textual.widgets import Button, Label, Static

# All cache/config files speek-max uses
_CACHE_FILES: list[tuple[str, str]] = [
    ('~/.config/speek/system_probe.json',      'SLURM capability probe results'),
    ('~/.config/speek-max/settings.json',       'User settings (theme, toggles, refresh rates)'),
    ('~/.config/speek-max/command_history.json', 'Shell command history'),
    ('~/.config/speek-max/commands.yaml',        'User-defined command aliases'),
    ('~/.cache/speek/history_read.json',         'Read/unread state for job events'),
    ('~/.config/speek/submit_store.json',        'Legacy submit widget store'),
]


# ── Consequence text per capability ──────────────────────────────────────────

_CMD_CONSEQUENCES = {
    'squeue':   'My Jobs, Queue tab, Priority popup',
    'scontrol': 'Node tab status, active job detail popup',
    'sacct':    'History, Users tab, Stats issues, completed job details',
    'sinfo':    'Partition list (Stats filter, job submission)',
    'sprio':    'Job priority scores in Priority popup',
    'sshare':   'Fairshare scores in Priority popup',
    'scancel':  'Job cancellation from My Jobs',
}

_FIELD_CONSEQUENCES = {
    'StdOut':     'Log path read directly from sacct',
    'StdErr':     'Stderr path available in job details',
    'SubmitLine': 'Log path inferred from sbatch command arguments',
}

_STRATEGY_LABELS = {
    'sacct_stdout':        'sacct StdOut field (direct)',
    'submit_line_parse':   'Parse --output from sbatch command',
    'filesystem_fallback': 'Filesystem pattern scan ({WorkDir}/out/{jobid}.out etc.)',
}

def _fmt_size(n: int) -> str:
    """Human-readable file size."""
    if n < 1024:
        return f'{n} B'
    if n < 1024 * 1024:
        return f'{n / 1024:.1f} KB'
    return f'{n / 1024 / 1024:.1f} MB'


def _short_path(raw: str) -> str:
    """Shorten ~/.config/speek-max/foo.json to ~/…/foo.json."""
    parts = raw.split('/')
    if len(parts) > 3:
        return f'{parts[0]}/…/{parts[-1]}'
    return raw


def _icon(ok: bool, tv: dict, warn: bool = False) -> str:
    from speek.speek_max._utils import tc
    c_ok   = tc(tv, 'text-success', 'green')
    c_warn = tc(tv, 'text-warning', 'yellow')
    c_fail = tc(tv, 'text-error',   'red')
    if ok:
        return f'[bold {c_ok}]✔[/]'
    return f'[bold {c_warn}]⚠[/]' if warn else f'[bold {c_fail}]✗[/]'


def _lat(ms: int, tv: dict) -> str:
    """Format a latency value with colour coding: green <50ms, yellow <300ms, red ≥300ms."""
    from speek.speek_max._utils import tc
    if ms <= 0:
        return ''
    if ms < 50:
        colour = tc(tv, 'text-success', 'green')
    elif ms < 300:
        colour = tc(tv, 'text-warning', 'yellow')
    else:
        colour = tc(tv, 'text-error',   'red')
    return f'[{colour}]{ms}ms[/]'


# ── Widget ───────────────────────────────────────────────────────────────────

class SysInfoWidget(Widget):
    """Displays SLURM capability probe results with consequences."""

    BORDER_TITLE = 'System Info'

    BINDINGS = [
        Binding('ctrl+r', 'reprobe', 'Re-probe', show=True),
    ]

    DEFAULT_CSS = """
    SysInfoWidget {
        height: 1fr;
        border: tall $accent;
        border-title-color: $background;
        border-title-background: $accent;
        border-title-style: bold;
        padding: 0;
    }
    SysInfoWidget VerticalScroll { height: 1fr; padding: 0 1; }

    .si-section-title {
        color: $accent;
        text-style: bold;
        margin-top: 1;
    }
    .si-row { height: 1; }
    .si-note { color: $text-muted; text-style: italic; height: auto; }
    .si-kv   { height: 1; }
    .si-actions { height: 3; margin-top: 1; align: left middle; }
    .si-actions Button { min-width: 22; height: 1; border: none; }
    .si-age  { color: $text-muted; height: 1; }
    """

    def compose(self) -> ComposeResult:
        with VerticalScroll():
            yield Static('', id='si-content', markup=True)
            yield Static('', id='si-cache-info', markup=True)
            with Vertical(classes='si-actions'):
                yield Button('Re-probe  Ctrl+R', id='si-reprobe')
                yield Button('Delete all cache', id='si-delete-cache')
            yield Static('', id='si-age', classes='si-age', markup=True)

    def on_mount(self) -> None:
        self._render_results()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'si-reprobe':
            self.action_reprobe()
        elif event.button.id == 'si-delete-cache':
            self._confirm_delete_cache()

    def action_reprobe(self) -> None:
        self.query_one('#si-content', Static).update('[dim]Probing SLURM cluster…[/]')
        self.run_worker(self._run_probe, thread=True, group='reprobe')

    def _run_probe(self) -> None:
        from speek.speek_max.probe import get_probe_results
        from speek.speek_max import slurm as _slurm
        results = get_probe_results(force=True)
        _slurm.apply_probe(results)
        self.app.call_from_thread(self._render_results)

    def _render_results(self) -> None:
        from speek.speek_max.probe import load_cached_probe, get_probe_results, cache_age_str
        tv = self.app.theme_variables
        probe = load_cached_probe()
        if probe is None:
            # First launch with no cache — show loading state; probe runs async
            self.query_one('#si-content', Static).update(
                '[dim]No probe cache — probing SLURM cluster for the first time…[/]'
            )
            self.query_one('#si-age', Static).update('')
            return

        lines: List[str] = []
        W_NAME = 12  # column width for names
        W_FEAT = 30  # column width for feature/field names

        # ── Cluster info ──────────────────────────────────────────────────────
        lines.append('[bold $accent]── Cluster ──[/]')
        cluster = probe.get('cluster', {})
        for label, key in [
            ('Cluster',   'cluster_name'),
            ('SLURM',     'slurm_version'),
            ('Scheduler', 'scheduler'),
            ('Priority',  'priority_type'),
            ('Select',    'select_type'),
        ]:
            val = cluster.get(key, '—')
            lines.append(f'  [dim]{label:<{W_NAME}}[/] {val}')

        # ── GPU Hardware ──────────────────────────────────────────────────────
        lines.append('')
        lines.append('[bold $accent]── GPU Hardware ──[/]')
        try:
            from speek.speek_max.slurm import fetch_cluster_stats
            stats = fetch_cluster_stats()
            if stats:
                for model in sorted(stats, key=lambda m: stats[m]['Total'], reverse=True):
                    d = stats[model]
                    vram = d.get('VRAM')
                    cpu_pg = d.get('CPUperGPU')
                    ram_pg = d.get('RAMperGPU')
                    n_nodes = len(d.get('Nodes', []))
                    total = d['Total']
                    specs = []
                    if vram:
                        specs.append(f'VRAM {vram}GB')
                    if cpu_pg:
                        specs.append(f'{cpu_pg} CPUs/GPU')
                    if ram_pg:
                        specs.append(f'{ram_pg}GB RAM/GPU')
                    specs.append(f'{n_nodes} node{"s" if n_nodes != 1 else ""}')
                    specs.append(f'{total} GPUs')
                    lines.append(f'  [bold]{model:<{W_NAME}}[/bold] [dim]{" · ".join(specs)}[/dim]')
            else:
                lines.append('  [dim]No GPU data available[/]')
        except Exception:
            lines.append('  [dim]Could not fetch GPU info[/]')

        # ── Commands + latency ───────────────────────────────────────────────
        lines.append('')
        lines.append('[bold $accent]── SLURM Commands ──[/]')
        #              icon  name        latency  consequence
        cmds = probe.get('commands', {})
        for name, consequence in _CMD_CONSEQUENCES.items():
            entry = cmds.get(name, {})
            if isinstance(entry, dict):
                ok = entry.get('ok', False)
                ms = entry.get('ms', 0)
            else:
                ok, ms = bool(entry), 0
            icon = _icon(ok, tv)
            lat  = f'{_lat(ms, tv):>8}' if ok and ms else '        '
            if ok:
                lines.append(
                    f'  {icon}  [bold]{name:<{W_NAME}}[/bold]  {lat}  [dim]{consequence}[/dim]'
                )
            else:
                lines.append(
                    f'  {icon}  [dim bold]{name:<{W_NAME}}[/dim bold]  {lat}  [dim]{consequence}[/dim]'
                )

        # ── sacct capabilities ────────────────────────────────────────────────
        lines.append('')
        lines.append('[bold $accent]── sacct Capabilities ──[/]')

        hist_entry = probe.get('sacct_history', probe.get('sacct_history_ok', False))
        if isinstance(hist_entry, dict):
            hist_ok = hist_entry.get('ok', False)
            hist_ms = hist_entry.get('ms', 0)
        else:
            hist_ok, hist_ms = bool(hist_entry), 0
        hist_note = 'Job history beyond current day' if hist_ok else 'Limited to current day'
        hist_lat = f'{_lat(hist_ms, tv):>8}' if hist_ok and hist_ms else '        '
        lines.append(
            f'  {_icon(hist_ok, tv)}  {"Extended history (-S)":<{W_FEAT}}  {hist_lat}  [dim]{hist_note}[/]'
        )

        sf = probe.get('sacct_fields', {})
        avail_set = set(sf.get('desired_available', []))
        for field, consequence in _FIELD_CONSEQUENCES.items():
            ok = field in avail_set
            if ok:
                lines.append(
                    f'  {_icon(ok, tv)}  {field:<{W_FEAT}}            [dim]{consequence}[/dim]'
                )
            else:
                lines.append(
                    f'  {_icon(ok, tv)}  [dim]{field:<{W_FEAT}}            {consequence}[/dim]'
                )

        strategy = probe.get('log_path_strategy', 'filesystem_fallback')
        strategy_label = _STRATEGY_LABELS.get(strategy, strategy)
        lines.append(f'  [dim]{"Log strategy":<{W_FEAT}}            {strategy_label}[/]')

        # ── Missing desired fields ────────────────────────────────────────────
        missing = sf.get('desired_missing', [])
        if missing:
            lines.append('')
            lines.append('[bold $accent]── Unavailable sacct Fields ──[/]')
            lines.append(f'  [dim]{", ".join(missing)}[/]')
            lines.append('  [dim](falls back gracefully)[/]')

        self.query_one('#si-content', Static).update('\n'.join(lines))

        ts = probe.get('timestamp', 0)
        self.query_one('#si-age', Static).update(
            f'[dim]Last probed: {cache_age_str()}  '
            f'({time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))})[/]'
        )

        # ── Cache info ─────────────────────────────────────────────────────
        self._render_cache_info()

    def _render_cache_info(self) -> None:
        tv = self.app.theme_variables
        from speek.speek_max._utils import tc
        c_ok = tc(tv, 'text-success', 'green')
        c_dim = tc(tv, 'text-muted', 'bright_black')

        lines: List[str] = ['', '[bold $accent]── Cache & Config Files ──[/]']
        total_bytes = 0
        for raw_path, description in _CACHE_FILES:
            p = Path(raw_path).expanduser()
            if p.exists():
                size = p.stat().st_size
                total_bytes += size
                size_str = _fmt_size(size)
                lines.append(
                    f'  [{c_ok}]●[/]  {size_str:>8}  [dim]{_short_path(raw_path)}[/]'
                    f'  [{c_dim}]{description}[/]'
                )
            else:
                lines.append(
                    f'  [{c_dim}]○[/]  {"—":>8}  [dim]{_short_path(raw_path)}[/]'
                    f'  [{c_dim}]{description}[/]'
                )
        lines.append(f'  [bold]Total: {_fmt_size(total_bytes)}[/bold]')
        try:
            self.query_one('#si-cache-info', Static).update('\n'.join(lines))
        except Exception:
            pass

    def _confirm_delete_cache(self) -> None:
        from speek.speek_max.widgets.confirmation import ConfirmationModal

        def _on_confirm(confirmed: bool) -> None:
            if confirmed:
                self._delete_all_cache()

        self.app.push_screen(
            ConfirmationModal('Delete all speek-max cache and config files?'),
            _on_confirm,
        )

    def _delete_all_cache(self) -> None:
        deleted = 0
        for raw_path, _ in _CACHE_FILES:
            p = Path(raw_path).expanduser()
            try:
                if p.exists():
                    p.unlink()
                    deleted += 1
            except Exception:
                pass
        self.app.notify(
            f'Deleted {deleted} cache file(s)', title='Cache cleared')
        self._render_cache_info()
