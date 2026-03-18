"""cluster_bar.py — Always-visible GPU stats bar at top of screen."""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

from rich.table import Table
from rich.text import Text
from textual.widgets import Static

from speek.speek_max.slurm import fetch_cluster_stats, fetch_job_stats
from speek.speek_max._utils import tc


_NODE_NUM_RE = re.compile(r'^(.*?)(\d+)$')

_STATE_COLORS = {
    'IDLE':       'text-success',
    'ALLOCATED':  'text-error',
    'MIXED':      'text-warning',
    'COMPLETING': 'text-warning',
    'DRAINING':   'text-error',
    'DRAINED':    'text-muted',
    'DOWN':       'text-muted',
}
_STATE_FALLBACKS = {
    'IDLE':       '#00FA9A',
    'ALLOCATED':  '#FF4500',
    'MIXED':      '#FFD700',
    'COMPLETING': '#FFD700',
    'DRAINING':   '#FF4500',
    'DRAINED':    'bright_black',
    'DOWN':       'bright_black',
}


def _node_range_text(nodes_states: List[Tuple[str, str]], tv: dict) -> Text:
    """Render [(name, state)] as prefix-once ranges, e.g. gpu1~3,5,7~9."""
    muted = tc(tv, 'text-muted', 'bright_black')

    # parse into (prefix, num, state)
    parsed = []
    for name, state in nodes_states:
        m = _NODE_NUM_RE.match(name)
        if m:
            parsed.append((m.group(1), int(m.group(2)), state))
        else:
            parsed.append((name, -1, state))

    parsed.sort(key=lambda x: (x[0], x[1]))

    # group by prefix first, then build contiguous same-state runs within prefix
    from itertools import groupby
    t = Text()
    prefix_groups = {}
    for prefix, num, state in parsed:
        prefix_groups.setdefault(prefix, []).append((num, state))

    first_prefix = True
    total_ranges = 0
    MAX_RANGES = 6

    for prefix, items in prefix_groups.items():
        # build contiguous runs per state
        runs: List[Tuple[int, int, str]] = []  # (start, end, state)
        for num, state in items:
            if num == -1:
                runs.append((-1, -1, state))
                continue
            if runs and runs[-1][2] == state and runs[-1][1] == num - 1:
                runs[-1] = (runs[-1][0], num, state)
            else:
                runs.append((num, num, state))

        if not first_prefix:
            t.append(' ', style=muted)
        t.append(prefix, style=muted)
        first_prefix = False

        for i, (start, end, state) in enumerate(runs):
            if total_ranges >= MAX_RANGES:
                remaining = sum(len(v) for v in prefix_groups.values()) - total_ranges
                t.append(f'+{remaining}', style=muted)
                return t
            color = tc(tv, _STATE_COLORS.get(state, 'text-muted'),
                       _STATE_FALLBACKS.get(state, 'bright_black'))
            if i:
                t.append(',', style=muted)
            if start == -1:
                pass  # prefix-only node already appended
            elif start == end:
                t.append(str(start), style=color)
            else:
                t.append(f'{start}:{end}', style=color)
            total_ranges += 1

    return t


def _usage_emoji(pct: float) -> str:
    """Same rule as speek/speek-min."""
    if pct >= 100: return '☠ '
    if pct > 90:   return '🔥'
    if pct == 0:   return '🏖 '
    if pct < 10:   return '❄ '
    return ''


def build_cluster_renderable(
    stats: Dict[str, Dict],
    job_stats: Dict,
    tv: Dict[str, str],
) -> object:
    if not stats:
        return Text("No GPU data", style=tc(tv, 'text-muted', 'bright_black') + ' italic')

    primary      = tc(tv, 'primary',      '#C45AFF')
    text_muted   = tc(tv, 'text-muted',   'bright_black')
    text_success = tc(tv, 'text-success', '#00FA9A')
    text_warning = tc(tv, 'text-warning', '#FFD700')
    text_error   = tc(tv, 'text-error',   '#FF4500')

    def _util_color(pct: float) -> str:
        if pct >= 0.90: return text_error
        if pct >= 0.50: return text_warning
        return text_success

    def _bar(used: int, total: int, width: int = 16) -> Text:
        pct = used / total if total else 0.0
        color = _util_color(pct)
        # dark text on bright green/yellow, white on red/orange
        fg = 'black' if pct < 0.90 else 'white'
        pct_str = f'{round(pct * 100)}%'
        fw = max(int(round(pct * width)), len(pct_str))
        aw = width - fw
        t = Text()
        t.append(f'|{pct_str:<{fw}}', style=f'bold {fg} on {color}')
        t.append(' ' * aw + '|',      style=text_muted)
        return t

    models = sorted(stats, key=lambda m: stats[m]['Total'], reverse=True)

    by_model = (job_stats or {}).get('by_model', {})

    table = Table(box=None, padding=(0, 0), show_header=False, expand=True)
    table.add_column('model',   no_wrap=True)
    table.add_column('emoji',   width=2,  no_wrap=True)
    table.add_column('bar',     no_wrap=True, min_width=20)
    table.add_column('counts',  no_wrap=True)
    table.add_column('demand',  no_wrap=True)
    table.add_column('n_count', no_wrap=True)
    table.add_column('nodes',   no_wrap=True, min_width=8, max_width=30)

    total_T = total_U = 0
    for m in models:
        d = stats[m]
        T, U, F = d['Total'], d['Used'], d['Free']
        pct = U / T if T else 0.0
        pct_100 = pct * 100
        total_T += T
        total_U += U
        uc = _util_color(pct)
        emoji = _usage_emoji(pct_100)

        vram = d.get('VRAM')
        vram_str = f' {vram}GB' if vram else ''

        model_cell = Text()
        model_cell.append(m,        style=f'bold {uc}')
        model_cell.append(vram_str, style=text_muted)

        nodes = d.get('Nodes', [])
        n_count = len(nodes)

        counts_cell = Text()
        counts_cell.append(f' {F}', style=f'bold {uc}')
        counts_cell.append('/', style=text_muted)
        counts_cell.append(str(T), style=text_muted)

        pending_jobs = by_model.get(m, {}).get('PD', 0)
        demand_cell = Text()
        if pending_jobs:
            pressure = pending_jobs / max(F, 1)
            dc = text_error if pressure >= 2 else text_warning if pressure >= 1 else text_muted
            demand_cell.append(f' ↑{pending_jobs}', style=dc)
        else:
            demand_cell.append('     ', style=text_muted)

        table.add_row(
            model_cell,
            Text(f'{emoji} '),
            _bar(U, T),
            counts_cell,
            demand_cell,
            Text(f' {n_count}× ', style=text_muted),
            _node_range_text(nodes, tv),
        )

    total_pct = total_U / total_T if total_T else 0.0
    uc = _util_color(total_pct)
    total_bg = tc(tv, 'surface', '#252119')
    total_counts = Text()
    total_counts.append(f' {total_T - total_U}', style=f'bold {uc}')
    total_counts.append('/', style=text_muted)
    total_counts.append(str(total_T), style=text_muted)
    total_pd = sum(by_model.get(m, {}).get('PD', 0) for m in models)
    total_demand = Text()
    if total_pd:
        total_pressure = total_pd / max(total_T - total_U, 1)
        dc = text_error if total_pressure >= 2 else text_warning if total_pressure >= 1 else text_muted
        total_demand.append(f' ↑{total_pd}', style=dc)
    table.add_row(
        Text('Total', style=f'bold {uc}'),
        Text(f'{_usage_emoji(total_pct * 100)} '),
        _bar(total_U, total_T),
        total_counts,
        total_demand,
        Text(''),
        Text(''),
        style=f'on {total_bg}',
    )

    return table


class ClusterBar(Static):
    """Always-visible cluster GPU availability bar. Refreshes every 10s."""

    BORDER_TITLE = "Cluster"

    def on_mount(self) -> None:
        self._refresh_data()
        self.set_interval(10, self._refresh_data)

    def _refresh_data(self) -> None:
        self.run_worker(self._fetch, thread=True, exclusive=True, group='cluster-bar')

    def _fetch(self) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        stats = fetch_cluster_stats()
        job_stats = fetch_job_stats()
        if not worker.is_cancelled:
            self.app.call_from_thread(self._update, stats, job_stats)

    def _update(self, stats: Dict[str, Dict], job_stats: Dict) -> None:
        self.update(build_cluster_renderable(stats, job_stats, self.app.theme_variables))
