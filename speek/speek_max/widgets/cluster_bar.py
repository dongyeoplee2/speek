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
    'IDLE':       'green',
    'ALLOCATED':  'red',
    'MIXED':      'yellow',
    'COMPLETING': 'yellow',
    'DRAINING':   'red',
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
    if pct >= 100: return '💀 '
    if pct > 90:   return '🔥 '
    if pct == 0:   return '🏖  '
    if pct < 10:   return '❄  '
    return '   '


def build_cluster_renderable(
    stats: Dict[str, Dict],
    job_stats: Dict,
    tv: Dict[str, str],
) -> object:
    if not stats:
        return Text("No GPU data", style=tc(tv, 'text-muted', 'bright_black') + ' italic')

    text_muted   = tc(tv, 'text-muted',   'bright_black')

    # Use each scheme's own success/warning/error — these are already
    # green, yellow/orange, red per the base16 mapping (base0B/base09/base08)
    _c_green  = tc(tv, 'success', 'green')
    _c_yellow = tc(tv, 'warning', 'yellow')
    _c_red    = '#CC3333'  # true red, not theme's pinkish error color

    def _util_color(pct: float) -> str:
        if pct >= 1.0:  return _c_red
        if pct >= 0.50: return _c_yellow
        return _c_green

    def _bar(used: int, total: int, width: int = 16, down: int = 0) -> Text:
        pct = used / total if total else 0.0
        down_pct = down / total if total else 0.0
        if pct == 0 and down == 0:
            t = Text()
            t.append('|', style=text_muted)
            t.append(' 0% ', style=f'bold {_c_green}')
            t.append(' ' * (width - 4) + '|', style=text_muted)
            return t
        color = _util_color(pct)
        fg = 'black' if pct < 0.90 else 'white'
        pct_str = f'{round(pct * 100)}%'
        fw = max(int(round(pct * width)), len(pct_str))
        dw = int(round(down_pct * width))
        aw = max(0, width - fw - dw)
        t = Text()
        t.append(f'|{pct_str:<{fw}}', style=f'bold {fg} on {color}')
        if aw > 0:
            t.append(' ' * aw, style=text_muted)
        if dw > 0:
            t.append('░' * dw, style='#333333')
        t.append('|', style=text_muted)
        return t

    models = sorted(stats, key=lambda m: stats[m]['Total'], reverse=True)

    by_model      = (job_stats or {}).get('by_model', {})

    table = Table(box=None, padding=(0, 0), show_header=False, expand=True)
    table.add_column('model',   no_wrap=True, min_width=8)
    table.add_column('vram',    no_wrap=True, width=5)
    table.add_column('emoji',   width=3,  no_wrap=True)
    table.add_column('bar',     no_wrap=True, min_width=20)
    table.add_column('counts',  no_wrap=True)
    table.add_column('demand',  no_wrap=True)
    table.add_column('n_count', no_wrap=True)
    table.add_column('nodes',   no_wrap=True, min_width=8, max_width=30)

    total_T = total_U = total_down = 0
    for m in models:
        d = stats[m]
        T, U, F = d['Total'], d['Used'], d['Free']
        pct = U / T if T else 0.0
        pct_100 = pct * 100
        total_T += T
        total_U += U
        down_gpus = d.get('DownGPUs', 0)
        total_down += down_gpus
        uc = _util_color(pct)
        emoji = _usage_emoji(pct_100)

        vram = d.get('VRAM')
        all_down = down_gpus >= T and T > 0

        if all_down:
            model_cell = Text(m, style='bright_black')
            vram_cell = Text(f'{vram}G', style='bright_black') if vram else Text('')
        else:
            model_cell = Text(m, style=f'bold {uc}')
            vram_cell = Text(f'{vram}G', style=text_muted) if vram else Text('')

        nodes = d.get('Nodes', [])
        n_count = len(nodes)

        counts_cell = Text()
        counts_cell.append(f' {F}', style=f'bold {uc}')
        counts_cell.append('/', style=text_muted)
        counts_cell.append(str(T), style=text_muted)

        pending_jobs = by_model.get(m, {}).get('PD', 0)
        demand_cell = Text()
        if all_down:
            demand_cell.append(f' ↓{down_gpus}', style='bold red')
        elif down_gpus > 0:
            demand_cell.append(f' ↓{down_gpus}', style='bold red')
        elif pending_jobs:
            pressure = pending_jobs / max(F, 1)
            if pressure >= 2:
                dc = _c_red
            elif pressure >= 1:
                dc = _c_yellow
            else:
                dc = text_muted
            demand_cell.append(f' ↑{pending_jobs}', style=dc)
        else:
            demand_cell.append('     ', style=text_muted)

        if all_down:
            _ds = 'bright_black'
            width = 16  # match _bar() default width
            dead_bar = Text(f'|{"DEAD":^{width}}|', style='bright_black on black')
            dead_cnt = Text()
            dead_cnt.append(f' {F}', style=_ds)
            dead_cnt.append('/', style=_ds)
            dead_cnt.append(str(T), style=_ds)
            table.add_row(
                model_cell, vram_cell, Text('', style=_ds),
                dead_bar, dead_cnt,
                Text('', style=_ds),
                Text(f' {n_count}× ', style=_ds),
                _node_range_text(nodes, tv),
            )
        else:
            table.add_row(
                model_cell, vram_cell,
                Text(f'{emoji} '),
                _bar(U - down_gpus, T, down=down_gpus),
                counts_cell, demand_cell,
                Text(f' {n_count}× ', style=text_muted),
                _node_range_text(nodes, tv),
            )

    total_pct = total_U / total_T if total_T else 0.0
    uc = _util_color(total_pct)
    total_bg = tc(tv, 'border-blurred', 'bright_black')
    total_counts = Text()
    total_counts.append(f' {total_T - total_U}', style=f'bold {uc}')
    total_counts.append('/', style=text_muted)
    total_counts.append(str(total_T), style=text_muted)
    total_pd = sum(by_model.get(m, {}).get('PD', 0) for m in models)
    total_demand = Text()
    if total_pd:
        total_pressure = total_pd / max(total_T - total_U, 1)
        if total_pressure >= 2:
            dc = _c_red
        elif total_pressure >= 1:
            dc = _c_yellow
        else:
            dc = text_muted
        total_demand.append(f' ↑{total_pd}', style=dc)
    table.add_row(
        Text('Total', style=f'bold {uc}'),
        Text(''),
        Text(f'{_usage_emoji(total_pct * 100)} '),
        _bar(total_U - total_down, total_T, down=total_down),
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
        tv = self.app.theme_variables
        self.run_worker(
            lambda: self._fetch(tv),
            thread=True, exclusive=True, group='cluster-bar',
        )

    def _fetch(self, tv: Dict) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        stats     = fetch_cluster_stats()
        job_stats = fetch_job_stats()
        if not worker.is_cancelled:
            renderable = build_cluster_renderable(stats, job_stats, tv)
            self.app.call_from_thread(self.update, renderable)
