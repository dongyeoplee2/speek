"""sysinfo_widget.py — System Info tab: SLURM capability probe results."""
from __future__ import annotations

import time
from pathlib import Path

from rich.table import Table as RichTable
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.widget import Widget
from textual.widgets import Button, Label, Static


def _kv_table(rows: list, key_width: int = 14) -> RichTable:
    """Build an invisible two-column table for key-value pairs."""
    t = RichTable(show_header=False, show_edge=False, box=None,
                  pad_edge=False, padding=(0, 1, 0, 0))
    t.add_column('key', style='dim', no_wrap=True, min_width=key_width, ratio=1)
    t.add_column('val', ratio=3)
    for k, v in rows:
        t.add_row(k, v)
    return t

# All cache/config files speek-max uses
_CACHE_FILES: list[tuple[str, str]] = [
    ('~/.config/speek/system_probe.json',      'SLURM capability probe results'),
    ('~/.config/speek-max/settings.json',       'User settings (theme, toggles, refresh rates)'),
    ('~/.config/speek-max/command_history.json', 'Shell command history'),
    ('~/.config/speek-max/commands.yaml',        'User-defined command aliases'),
    ('~/.cache/speek/history_read.json',         'Read/unread state for job events'),
    ('~/.cache/speek/oom_verdicts.json',         'OOM detection verdicts'),
    ('~/.cache/speek/job_transitions.json',      'Job state transition cache'),
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
    'sreport':  'User stats fallback (CPU hours when sacct unavailable)',
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
    SysInfoWidget VerticalScroll { height: 1fr; padding: 0 1; }

    .si-section-header {
        color: $primary;
        text-style: bold;
        margin-top: 1;
        width: 1fr;
        background: $background;
    }
    .si-card {
        background: $background;
        border: round $accent 40%;
        padding: 0 1;
        margin-bottom: 1;
        width: 1fr;
        height: auto;
    }
    .si-actions { height: 3; margin-top: 1; align: left middle; }
    .si-actions Button { min-width: 22; height: 1; border: none; }
    .si-age  { color: $text-muted; height: 1; }
    """

    _SI_SECTIONS = ['cluster', 'gpu', 'commands', 'datasources', 'sacct',
                     'missing', 'priority', 'myshare', 'errordetect', 'cache']

    def compose(self) -> ComposeResult:
        with VerticalScroll():
            for sec in self._SI_SECTIONS:
                yield Label('', id=f'si-header-{sec}', classes='si-section-header')
                with Vertical(classes='si-card', id=f'si-card-{sec}'):
                    yield Static('', id=f'si-{sec}', markup=True)
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
        try:
            self.query_one('#si-content', Static).update('[dim]Probing SLURM cluster…[/]')
        except Exception:
            pass
        self.run_worker(self._run_probe, thread=True, group='reprobe')

    def _run_probe(self) -> None:
        from speek.speek_max.probe import get_probe_results
        from speek.speek_max import slurm as _slurm
        _slurm.clear_all_caches()
        results = get_probe_results(force=True)
        _slurm.apply_probe(results)
        self.app.call_from_thread(self._render_results)

    def _render_results(self) -> None:
        from speek.speek_max.probe import load_cached_probe, get_probe_results, cache_age_str
        tv = self.app.theme_variables
        probe = load_cached_probe()
        if probe is None:
            self.query_one('#si-header-cluster', Label).update('── Cluster ──')
            self.query_one('#si-cluster', Static).update(
                '[dim]No probe cache — probing SLURM cluster for the first time…[/]'
            )
            self.query_one('#si-age', Static).update('')
            return

        W_NAME = 12

        # ── Cluster ──
        self.query_one('#si-header-cluster', Label).update('── Cluster ──')
        cluster = probe.get('cluster', {})
        cluster_rows = [
            (label, cluster.get(key, '—'))
            for label, key in [
                ('Cluster', 'cluster_name'), ('SLURM', 'slurm_version'),
                ('Scheduler', 'scheduler'), ('Priority', 'priority_type'),
                ('Select', 'select_type'),
            ]
        ]
        self.query_one('#si-cluster', Static).update(_kv_table(cluster_rows))

        # ── GPU Hardware ──
        self.query_one('#si-header-gpu', Label).update('── GPU Hardware ──')
        gpu_table = RichTable(show_header=True, show_edge=False, box=None,
                              pad_edge=False, padding=(0, 1, 0, 0))
        gpu_table.add_column('Model', style='bold', no_wrap=True, min_width=12)
        gpu_table.add_column('GPUs', justify='right', style='cyan', min_width=5)
        gpu_table.add_column('Nodes', justify='right', style='dim', min_width=5)
        gpu_table.add_column('VRAM', justify='right', min_width=6)
        gpu_table.add_column('CPU/GPU', justify='right', style='dim', min_width=7)
        gpu_table.add_column('RAM/GPU', justify='right', style='dim', min_width=7)
        try:
            from speek.speek_max.slurm import fetch_cluster_stats
            stats = fetch_cluster_stats()
            if stats:
                for model in sorted(stats, key=lambda m: stats[m]['Total'], reverse=True):
                    d = stats[model]
                    n_nodes = len(d.get('Nodes', []))
                    vram = d.get('VRAM')
                    cpu_pg = d.get('CPUperGPU')
                    ram_pg = d.get('RAMperGPU')
                    gpu_table.add_row(
                        model,
                        str(d['Total']),
                        str(n_nodes),
                        f'{vram}GB' if vram else '–',
                        str(cpu_pg) if cpu_pg else '–',
                        f'{ram_pg}GB' if ram_pg else '–',
                    )
            else:
                gpu_table.add_row('[dim]No GPU data available[/]', '', '', '', '', '')
        except Exception:
            gpu_table.add_row('[dim]Could not fetch GPU info[/]', '', '', '', '', '')
        self.query_one('#si-gpu', Static).update(gpu_table)

        # ── SLURM Commands ──
        self.query_one('#si-header-commands', Label).update('── SLURM Commands ──')
        cmd_table = RichTable(show_header=True, show_edge=False, box=None,
                              pad_edge=False, padding=(0, 1, 0, 0))
        cmd_table.add_column('', no_wrap=True, width=2)
        cmd_table.add_column('Command', style='bold', no_wrap=True, min_width=12)
        cmd_table.add_column('Latency', justify='right', min_width=8)
        cmd_table.add_column('Used by', style='dim')
        cmds = probe.get('commands', {})
        for name, consequence in _CMD_CONSEQUENCES.items():
            entry = cmds.get(name, {})
            if isinstance(entry, dict):
                ok = entry.get('ok', False)
                ms = entry.get('ms', 0)
            else:
                ok, ms = bool(entry), 0
            ico = _icon(ok, tv)
            lat = _lat(ms, tv) if ok and ms else ''
            name_str = name if ok else f'[dim]{name}[/dim]'
            cmd_table.add_row(ico, name_str, lat, consequence)
        self.query_one('#si-commands', Static).update(cmd_table)

        # ── Data Sources ──
        self.query_one('#si-header-datasources', Label).update('── Data Sources ──')
        ds_table = RichTable(show_header=True, show_edge=False, box=None,
                             pad_edge=False, padding=(0, 1, 0, 0))
        ds_table.add_column('Feature', style='bold', no_wrap=True, min_width=12)
        ds_table.add_column('Source', min_width=40)
        try:
            from speek.speek_max.slurm import get_data_source_levels
            sources = get_data_source_levels(self.app)
            for feature, (source, status) in sources.items():
                ico = _icon(status != 'unavailable', tv, warn=(status == 'limited'))
                suffix = ' [dim](limited)[/dim]' if status == 'limited' else ''
                ds_table.add_row(feature, f'{ico} {source}{suffix}')
        except Exception:
            ds_table.add_row('[dim]Could not determine data sources[/]', '')
        self.query_one('#si-datasources', Static).update(ds_table)

        # ── sacct Capabilities ──
        self.query_one('#si-header-sacct', Label).update('── sacct Capabilities ──')
        sacct_table = RichTable(show_header=True, show_edge=False, box=None,
                                pad_edge=False, padding=(0, 1, 0, 0))
        sacct_table.add_column('', no_wrap=True, width=2)
        sacct_table.add_column('Field', no_wrap=True, min_width=24)
        sacct_table.add_column('Latency', justify='right', min_width=8)
        sacct_table.add_column('Note', style='dim')
        hist_entry = probe.get('sacct_history', probe.get('sacct_history_ok', False))
        if isinstance(hist_entry, dict):
            hist_ok = hist_entry.get('ok', False)
            hist_ms = hist_entry.get('ms', 0)
        else:
            hist_ok, hist_ms = bool(hist_entry), 0
        hist_note = 'Job history beyond current day' if hist_ok else 'Limited to current day'
        hist_lat = _lat(hist_ms, tv) if hist_ok and hist_ms else ''
        sacct_table.add_row(_icon(hist_ok, tv), 'Extended history (-S)', hist_lat, hist_note)
        sf = probe.get('sacct_fields', {})
        avail_set = set(sf.get('desired_available', []))
        for field, consequence in _FIELD_CONSEQUENCES.items():
            ok = field in avail_set
            field_str = field if ok else f'[dim]{field}[/dim]'
            sacct_table.add_row(_icon(ok, tv), field_str, '', consequence)
        strategy = probe.get('log_path_strategy', 'filesystem_fallback')
        strategy_label = _STRATEGY_LABELS.get(strategy, strategy)
        sacct_table.add_row('', '[dim]Log strategy[/dim]', '', strategy_label)
        self.query_one('#si-sacct', Static).update(sacct_table)

        # ── Missing fields ──
        missing = sf.get('desired_missing', [])
        if missing:
            self.query_one('#si-header-missing', Label).update('── Unavailable sacct Fields ──')
            self.query_one('#si-missing', Static).update(
                f'[dim]{", ".join(missing)}[/]\n[dim](falls back gracefully)[/]'
            )
            self.query_one(f'#si-card-missing').display = True
        else:
            self.query_one('#si-header-missing', Label).update('')
            self.query_one(f'#si-card-missing').display = False

        ts = probe.get('timestamp', 0)
        self.query_one('#si-age', Static).update(
            f'[dim]Last probed: {cache_age_str()}  '
            f'({time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))})[/]'
        )

        # ── Scheduling Factors ─────────────────────────────────────────────
        self._render_priority_section(tv)

        # ── My Scheduling State ───────────────────────────────────────────
        self._render_myshare_section(tv)

        # ── Error Detection Rules ─────────────────────────────────────────
        self._render_error_detect_section()

        # ── Cache info ─────────────────────────────────────────────────────
        self._render_cache_info()

    def _render_priority_section(self, tv: dict) -> None:
        """Render the Scheduling Factors section using fetch_priority_config."""
        self.query_one('#si-header-priority', Label).update('── Scheduling Factors ──')
        # Load in background since it runs subprocess
        self.run_worker(self._fetch_and_render_priority, thread=True, group='si-priority')

    def _fetch_and_render_priority(self) -> None:
        try:
            from speek.speek_max.slurm import fetch_priority_config, describe_priority_factors
            config = fetch_priority_config()
            factors = describe_priority_factors(config)
        except Exception:
            config = None
            factors = None
        self.app.call_from_thread(self._apply_priority_section, config, factors)

    def _apply_priority_section(self, config: dict | None, factors: list | None) -> None:
        from speek.speek_max._utils import tc
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')

        if factors is None:
            self.query_one('#si-priority', Static).update(
                f'[{c_muted}]Priority config unavailable (sprio/scontrol not available)[/]'
            )
            return

        table = RichTable(show_header=True, show_edge=False, box=None,
                          pad_edge=False, padding=(0, 1, 0, 0))
        table.add_column('Factor', style='bold', no_wrap=True, min_width=12)
        table.add_column('Weight', justify='right', min_width=6)
        table.add_column('Description', style='dim')

        for f in factors:
            # factors are tuples: (name, weight_str, description)
            name, weight, desc = f[0], str(f[1]), f[2]
            if weight in ('0', 'None', ''):
                table.add_row(f'[dim]{name}[/dim]', f'[dim]{weight}[/dim]',
                              f'[dim](disabled)[/dim]')
            else:
                table.add_row(name, weight, desc)

        # Append config metadata
        if config:
            decay = config.get('decay_half_life', '—')
            max_age = config.get('max_age', '—')
            reset = config.get('usage_reset', 'never')
            table.add_row('', '', '')
            table.add_row(f'[{c_muted}]Decay half-life[/]', '', f'[{c_muted}]{decay}[/]')
            table.add_row(f'[{c_muted}]Max age bonus[/]', '', f'[{c_muted}]{max_age}[/]')
            table.add_row(f'[{c_muted}]Usage reset[/]', '', f'[{c_muted}]{reset}[/]')

        try:
            self.query_one('#si-priority', Static).update(table)
        except Exception:
            pass

    def _render_myshare_section(self, tv: dict) -> None:
        """Render the My Scheduling State section using fetch_user_share."""
        self.query_one('#si-header-myshare', Label).update('── My Scheduling State ──')
        # Load in background since it runs subprocess
        self.run_worker(self._fetch_and_render_myshare, thread=True, group='si-myshare')

    def _fetch_and_render_myshare(self) -> None:
        try:
            from speek.speek_max.slurm import fetch_user_share
            user = getattr(self.app, 'user', '')
            share = fetch_user_share(user)
        except Exception:
            share = None
        self.app.call_from_thread(self._apply_myshare_section, share)

    def _apply_myshare_section(self, share: dict | None) -> None:
        from speek.speek_max._utils import tc
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        c_error = tc(tv, 'text-error', 'red')

        if share is None:
            self.query_one('#si-myshare', Static).update(
                f'[{c_muted}]Scheduling state unavailable (sshare not available)[/]'
            )
            return

        fs = share.get('fairshare', None)
        eff_usage = share.get('effective_usage', None)
        fair_alloc = share.get('fair_allocation', None)
        usage_ratio = share.get('usage_ratio', None)
        recovery = share.get('recovery_estimate', None)

        table = RichTable(show_header=True, show_edge=False, box=None,
                          pad_edge=False, padding=(0, 1, 0, 0))
        table.add_column('Metric', style='bold', no_wrap=True, min_width=16)
        table.add_column('Value', justify='right', min_width=10)
        table.add_column('Note', style='dim')

        # Fairshare
        if fs is not None:
            fs_color = c_success if fs >= 0.5 else c_warning if fs >= 0.2 else c_error
            table.add_row('Fairshare', f'[{fs_color}]{fs:.3f}[/]',
                          'Higher is better (0–1)')
        else:
            table.add_row('Fairshare', f'[{c_muted}]—[/]', '')

        # Effective usage
        if eff_usage is not None:
            table.add_row('Eff. usage', f'{eff_usage:.1f}%', '')
        else:
            table.add_row('Eff. usage', f'[{c_muted}]—[/]', '')

        # Fair allocation
        if fair_alloc is not None:
            table.add_row('Fair alloc.', f'{fair_alloc:.1f}%', '')
        else:
            table.add_row('Fair alloc.', f'[{c_muted}]—[/]', '')

        # Usage ratio
        if usage_ratio is not None:
            ratio_color = c_error if usage_ratio > 2.0 else c_warning if usage_ratio > 1.0 else c_success
            table.add_row('Usage ratio', f'[{ratio_color}]{usage_ratio:.1f}x[/]',
                          'Your use vs. your share')
        else:
            table.add_row('Usage ratio', f'[{c_muted}]—[/]', '')

        # Recovery
        if recovery is not None:
            table.add_row('Recovery', f'[{c_muted}]{recovery}[/]',
                          'Est. time to restore fairshare')
        else:
            table.add_row('Recovery', f'[{c_muted}]—[/]', '')

        try:
            self.query_one('#si-myshare', Static).update(table)
        except Exception:
            pass

    def _render_error_detect_section(self) -> None:
        """Render the Error Detection Rules table."""
        self.query_one('#si-header-errordetect', Label).update('── Error Detection Rules ──')
        from speek.speek_max.log_scan import _ERROR_PATTERNS

        table = RichTable(show_header=True, show_edge=False, box=None,
                          pad_edge=False, padding=(0, 1, 0, 0))
        table.add_column('Type', style='bold', no_wrap=True, min_width=14)
        table.add_column('Description', min_width=24)
        table.add_column('Suggestions', style='dim')

        _ICONS = {
            'OOM': '☢', 'NCCL': '🔗', 'CUDA_ERROR': '⚡', 'NAN_LOSS': '∅',
            'SHAPE_MISMATCH': '▦', 'FILE_ERROR': '📁', 'DIST_TIMEOUT': '⏱',
            'SEGFAULT': '💥', 'PREEMPTED': '⏏', 'IMPORT_ERROR': '📦', 'KILLED': '☠',
        }

        for _, etype, desc, suggestions in _ERROR_PATTERNS:
            icon = _ICONS.get(etype, '⚠')
            table.add_row(
                f'{icon} {etype}',
                desc,
                suggestions[0] if suggestions else '',
            )

        try:
            self.query_one('#si-errordetect', Static).update(table)
        except Exception:
            pass

    def _render_cache_info(self) -> None:
        tv = self.app.theme_variables
        from speek.speek_max._utils import tc
        c_ok = tc(tv, 'text-success', 'green')
        c_dim = tc(tv, 'text-muted', 'bright_black')

        self.query_one('#si-header-cache', Label).update('── Cache & Config Files ──')
        cache_table = RichTable(show_header=True, show_edge=False, box=None,
                                pad_edge=False, padding=(0, 1, 0, 0))
        cache_table.add_column('', no_wrap=True, width=2)
        cache_table.add_column('Size', justify='right', min_width=8)
        cache_table.add_column('Path', style='dim', no_wrap=True)
        cache_table.add_column('Description', style='dim')

        # In-memory cache stats
        try:
            from speek.speek_max.slurm import get_cache_stats
            stats = get_cache_stats()
            total_entries = sum(stats.values())
            cache_table.add_row(
                f'[{c_ok}]●[/]', str(total_entries),
                'In-memory', f'[{c_dim}]entries across {len(stats)} caches[/]',
            )
        except Exception:
            pass

        # Persistent cache files
        total_bytes = 0
        for raw_path, description in _CACHE_FILES:
            p = Path(raw_path).expanduser()
            if p.exists():
                size = p.stat().st_size
                total_bytes += size
                cache_table.add_row(
                    f'[{c_ok}]●[/]', _fmt_size(size),
                    _short_path(raw_path), f'[{c_dim}]{description}[/]',
                )
            else:
                cache_table.add_row(
                    f'[{c_dim}]○[/]', '—',
                    _short_path(raw_path), f'[{c_dim}]{description}[/]',
                )
        cache_table.add_row('', f'[bold]{_fmt_size(total_bytes)}[/bold]', '[bold]Total[/bold]', '')

        # Last cleared timestamp
        try:
            from speek.speek_max.slurm import get_last_cache_clear
            ts = get_last_cache_clear()
            if ts is not None:
                import time as _time
                ago = _time.monotonic() - ts
                if ago < 60:
                    label = f'{int(ago)}s ago'
                elif ago < 3600:
                    label = f'{int(ago / 60)}m ago'
                else:
                    label = f'{int(ago / 3600)}h ago'
                cache_table.add_row(
                    '', '', f'[{c_dim}]Last cleared[/]', f'[{c_dim}]{label}[/]',
                )
        except Exception:
            pass

        try:
            self.query_one('#si-cache', Static).update(cache_table)
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
        # Clear in-memory caches first
        try:
            from speek.speek_max.slurm import clear_all_caches
            clear_all_caches()
        except Exception:
            pass
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
            f'Deleted {deleted} cache file(s) + in-memory caches', title='Cache cleared')
        self._render_cache_info()
