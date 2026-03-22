"""stats_widget.py — GPU time-usage statistics: sparkline timeline + breakdown table."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from rich.text import Text
from rich.table import Table
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widget import Widget
from textual.widgets import Button, Input, Label, Select, Sparkline, Static

from speek.speek_max.widgets.select import SpeekSelect
from speek.speek_max._utils import safe

_LOADING_ID  = '#stats-lbl-loading'
_FILTER_BAR  = '#stats-filter-bar'
_ISSUES_ID   = '#stats-issues'
_SPARKLINE_ID  = '#stats-sparkline'
_HOVER_INFO_ID = '#stats-hover-info'
_STACKED_ID    = '#stats-user-chart'
_LEGEND_ID     = '#stats-user-legend'

# ── User color palette for stacked chart ──────────────────────────────────────

_USER_COLORS = [
    '#4A9FD9',  # blue
    '#D9534F',  # red
    '#5CB85C',  # green
    '#F0AD4E',  # orange
    '#9B59B6',  # purple
    '#1ABC9C',  # teal
    '#E74C3C',  # crimson
    '#3498DB',  # sky blue
    '#2ECC71',  # emerald
    '#E67E22',  # carrot
    '#8E44AD',  # amethyst
    '#34495E',  # wet asphalt
]


def _render_stacked_chart(per_group_ts: Dict, peak: float, n_buckets: int) -> Text:
    """Render a stacked bar chart as Rich Text with per-user colors."""
    if not per_group_ts or peak <= 0:
        return Text('')

    # Sort users by total usage (descending) for consistent color assignment
    users = sorted(
        per_group_ts.keys(),
        key=lambda u: sum(per_group_ts[u].get('buckets', [])),
        reverse=True,
    )
    user_color = {u: _USER_COLORS[i % len(_USER_COLORS)] for i, u in enumerate(users)}

    height = 8  # rows of block characters — matches sparkline visual height
    # Source bucket count from actual data
    src_len = max((len(d.get('buckets', [])) for d in per_group_ts.values()), default=n_buckets)
    display_w = n_buckets  # this is the actual widget width

    # Pre-compute resampled per-column data
    col_data: List[List[Tuple[str, float]]] = []
    for col in range(display_w):
        src_s = int(col * src_len / display_w)
        src_e = int((col + 1) * src_len / display_w)
        src_e = max(src_e, src_s + 1)
        segments = []
        for u in users:
            bkts = per_group_ts[u].get('buckets', [])
            val = sum(bkts[i] for i in range(src_s, min(src_e, len(bkts))))
            val /= max(1, src_e - src_s)
            if val > 0:
                segments.append((u, val))
        col_data.append(segments)

    lines: List[Text] = []
    for row in range(height - 1, -1, -1):
        line = Text()
        row_bottom = (row / height) * peak
        row_top = ((row + 1) / height) * peak

        for col in range(display_w):
            segments = col_data[col]
            total = sum(v for _, v in segments)

            if total <= row_bottom:
                line.append(' ')
                continue

            # Walk through stacked users to find who occupies this row cell
            cumulative = 0.0
            cell_color = user_color[users[-1]]  # fallback
            # Determine the midpoint of the visible portion in this row
            visible_mid = max(row_bottom, 0) + (min(total, row_top) - max(row_bottom, 0)) / 2

            cumulative = 0.0
            for u, val in segments:
                cumulative += val
                if cumulative > visible_mid:
                    cell_color = user_color[u]
                    break

            # Choose block character based on fill fraction within this row
            fill = min(total, row_top) - row_bottom
            frac = fill / (row_top - row_bottom) if row_top > row_bottom else 0
            blocks = ' ▁▂▃▄▅▆▇█'
            idx = min(int(frac * 8 + 0.5), 8)
            line.append(blocks[idx], style=cell_color)

        lines.append(line)

    result = Text('\n').join(lines)
    return result


def _build_user_legend(per_group_ts: Dict) -> Text:
    """Build a color legend line for per-user stacked chart."""
    users = sorted(
        per_group_ts.keys(),
        key=lambda u: sum(per_group_ts[u].get('buckets', [])),
        reverse=True,
    )
    user_color = {u: _USER_COLORS[i % len(_USER_COLORS)] for i, u in enumerate(users)}
    legend = Text()
    for i, u in enumerate(users):
        if i > 0:
            legend.append('  ')
        legend.append('●', style=user_color[u])
        legend.append(f' {u}')
    return legend

# ── Time-range presets ─────────────────────────────────────────────────────────

# label → (delta, n_buckets, strftime_format)
_RANGES: Dict[str, Tuple[timedelta, int, str]] = {
    '1h':  (timedelta(hours=1),   60, '%H:%M'),
    '6h':  (timedelta(hours=6),   72, '%H:%M'),
    '12h': (timedelta(hours=12),  72, '%H:%M'),
    '1d':  (timedelta(days=1),    96, '%H:%M'),
    '3d':  (timedelta(days=3),    72, '%m/%d %H'),
    '7d':  (timedelta(days=7),    84, '%m/%d'),
    '30d': (timedelta(days=30),   90, '%m/%d'),
}

_DIMS = ('cluster', 'partition', 'node', 'user')

_DIM_LABELS = {
    'cluster':   'GPU Model',
    'partition': 'Partition',
    'node':      'Node',
    'user':      'User',
}


# ── Breakdown table renderer ───────────────────────────────────────────────────

def _build_y_axis(peak: float, height: int, width: int = 7) -> Text:
    """Y-axis tick labels aligned to a sparkline of *height* character rows.

    Renders 3 ticks: peak at row 0, peak/2 at the midpoint, 0 at the last row.
    Each line is *width* characters wide (number right-justified + tick glyph).
    """
    if height <= 0 or peak <= 0:
        return Text('')
    bar_w = width - 1          # chars available for the numeric label
    ticks: Dict[int, str] = {
        0:           f'{peak:.0f}',
        height // 2: f'{peak / 2:.0f}',
        height - 1:  '0',
    }
    t = Text()
    for row in range(height):
        if row in ticks:
            suffix = '┘' if row == height - 1 else '┤'
            t.append(ticks[row].rjust(bar_w) + suffix, style='dim')
        else:
            t.append(' ' * bar_w + '│', style='dim')
        if row < height - 1:
            t.append('\n')
    return t


def _build_breakdown(rows: List[Dict], dim: str) -> Table:
    col = _DIM_LABELS.get(dim, 'Group')
    t = Table(box=None, show_header=True, expand=True,
              padding=(0, 1), show_edge=False)
    t.add_column(col,       style='bold',  min_width=14, no_wrap=True)
    t.add_column('GPU·h',   justify='right', style='green',  min_width=8)
    t.add_column('Jobs',    justify='right', style='cyan',   min_width=6)
    t.add_column('',        min_width=24)   # bar

    if not rows:
        t.add_row('(no data)', '', '', '')
        return t

    max_h = max(r['gpu_hours'] for r in rows) or 1
    for r in rows[:14]:
        filled = int(r['gpu_hours'] / max_h * 24)
        bar = f'[green]{"█" * filled}[/green][dim]{"░" * (24 - filled)}[/dim]'
        t.add_row(r['name'], f"{r['gpu_hours']:.1f}", str(r['jobs']), bar)
    return t


def _issue_color(n: int) -> str:
    if n == 0:   return 'dim'
    if n <= 3:   return 'yellow'
    if n <= 10:  return 'bold yellow'
    return 'bold red'



def _build_issues(data: Dict, hours: int) -> Table:
    """Clean issues overview: summary header + per-partition bars + per-node bars."""
    by_model = data.get('by_model', {})
    by_node  = data.get('by_node',  {})
    BAR_W = 20

    t = Table(box=None, show_header=False, expand=True,
              padding=(0, 1), show_edge=False)
    t.add_column('', min_width=10, max_width=14, no_wrap=True)  # name
    t.add_column('', justify='right', min_width=4)  # F
    t.add_column('', justify='right', min_width=4)  # T
    t.add_column('', justify='right', min_width=4)  # O
    t.add_column('', justify='right', min_width=4)  # ☢ (log OOM)
    t.add_column('', min_width=BAR_W)  # bar

    if not by_model and not by_node:
        t.add_row(
            Text(f'No issues in the last {hours}h', style='dim green'),
            Text(''), Text(''), Text(''), Text(''), Text('✓', style='bold green'),
        )
        return t

    # Totals
    total_f = sum(d.get('failed', 0) for d in by_model.values())
    total_t = sum(d.get('timeout', 0) for d in by_model.values())
    total_o = sum(d.get('oom', 0) for d in by_model.values())
    total_ol = sum(d.get('oom_log', 0) for d in by_model.values())
    total_all = total_f + total_t + total_o + total_ol

    # Summary header — put in the bar column to avoid stretching col 0
    summary = Text()
    summary.append(f'{total_all}', style='bold red' if total_all else 'bold green')
    summary.append(f' issues ', style='dim')
    if total_ol:
        summary.append(f'({total_ol} ☢) ', style='bold red')
    summary.append(f'(last {hours}h)', style='dim')
    t.add_row(Text(''), Text(''), Text(''), Text(''), Text(''), summary)

    # Column labels
    t.add_row(
        Text('Partition', style='bold'),
        Text('F', style='bold red'),
        Text('T', style='bold yellow'),
        Text('O', style='bold #ff00ff'),
        Text('☢', style='bold red'),
        Text('', style='dim'),
    )

    max_total = max((d.get('total', 0) for d in by_model.values()), default=1) or 1
    def _issue_row(name, d, max_val, bold=True):
        f = d.get('failed', 0)
        to = d.get('timeout', 0)
        o = d.get('oom', 0)
        ol = d.get('oom_log', 0)
        tot = d.get('total', 0)
        bar = Text()
        w_total = max(1, int(round(tot / max_val * BAR_W))) if tot else 0
        if w_total:
            w_f = int(round(f / tot * w_total)) if tot else 0
            w_ol = int(round(ol / tot * w_total)) if tot else 0
            w_o = int(round(o / tot * w_total)) if tot else 0
            w_t = w_total - w_f - w_o - w_ol
            if w_f > 0: bar.append('█' * w_f, style='red')
            if w_t > 0: bar.append('█' * w_t, style='yellow')
            if w_o > 0: bar.append('█' * w_o, style='#ff00ff')
            if w_ol > 0: bar.append('█' * w_ol, style='bold red')
        bar.append('░' * (BAR_W - w_total), style='dim')
        t.add_row(
            Text(name, style='bold' if bold else ''),
            Text(str(f), style=_issue_color(f)) if f else Text('·', style='dim'),
            Text(str(to), style=_issue_color(to)) if to else Text('·', style='dim'),
            Text(str(o), style=_issue_color(o)) if o else Text('·', style='dim'),
            Text(str(ol), style='bold red') if ol else Text('·', style='dim'),
            bar,
        )

    for name in sorted(by_model, key=lambda k: by_model[k].get('total', 0), reverse=True):
        _issue_row(name, by_model[name], max_total)

    if by_node:
        t.add_row(Text(''), Text(''), Text(''), Text(''), Text(''), Text(''))
        t.add_row(
            Text('Node', style='bold'),
            Text('F', style='bold red'),
            Text('T', style='bold yellow'),
            Text('O', style='bold #ff00ff'),
            Text('☢', style='bold red'),
            Text('', style='dim'),
        )
        max_node = max((d.get('total', 0) for d in by_node.values()), default=1) or 1
        for name in sorted(by_node, key=lambda k: by_node[k].get('total', 0), reverse=True)[:8]:
            _issue_row(name, by_node[name], max_node, bold=False)

    # Legend
    t.add_row(Text(''), Text(''), Text(''), Text(''), Text(''), Text(''))
    legend = Text()
    legend.append('█', style='red')
    legend.append(' Failed  ', style='dim')
    legend.append('█', style='yellow')
    legend.append(' Timeout  ', style='dim')
    legend.append('█', style='#ff00ff')
    legend.append(' OOM  ', style='dim')
    legend.append('█', style='bold red')
    legend.append(' ☢ Log OOM', style='dim')
    t.add_row(Text(''), Text(''), Text(''), Text(''), Text(''), legend)

    return t


# ── Widget ─────────────────────────────────────────────────────────────────────

class StatsWidget(Widget):
    """GPU time-usage statistics: timeline sparkline + per-dimension breakdown."""

    BORDER_TITLE = 'Stats'

    BINDINGS = [
        Binding('r', 'refresh_data', 'Refresh', show=True),
    ]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._dim          = 'cluster'
        self._range_key    = '7d'
        self._filter_val   = ''
        self._custom_start = ''
        self._custom_end   = ''
        self._last_peak    = 0.0
        self._ts_buckets:  List[float] = []
        self._ts_labels:   List[str]   = []
        self._last_hover_idx: int = -1
        self._w_sparkline: Sparkline | None = None
        self._w_hover:     Static    | None = None

    # ── Compose ────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        # ── toolbar ───────────────────────────────────────────────────────────
        with Horizontal(id='stats-toolbar'):
            for d in _DIMS:
                yield Button(d.capitalize(), id=f'stats-dim-{d}',
                             classes='stats-dim-btn')
            yield Button('Issues', id='stats-dim-issues', classes='stats-dim-btn')
            yield Label('·', classes='stats-sep')
            yield SpeekSelect(
                [(rk, rk) for rk in _RANGES] + [('Custom', 'custom')],
                value='7d',
                id='stats-range-select',
                allow_blank=False,
            )
            yield Label('', id='stats-toolbar-spacer')
            yield Button('⟳', id='stats-refresh-btn',
                         classes='stats-icon-btn')

        # ── filter bar (hidden for cluster) ──────────────────────────────────
        with Horizontal(id='stats-filter-bar'):
            yield Label('Filter:', classes='stats-lbl')
            yield SpeekSelect([('—', '—')], id='stats-filter-sel', allow_blank=False)

        # ── custom date bar (hidden unless range=custom) ──────────────────────
        with Horizontal(id='stats-custom-bar'):
            yield Label('From:', classes='stats-lbl')
            yield Input(placeholder='YYYY-MM-DD', id='stats-cust-start',
                        classes='stats-date-input')
            yield Label('To:', classes='stats-lbl')
            yield Input(placeholder='YYYY-MM-DD', id='stats-cust-end',
                        classes='stats-date-input')
            yield Button('Apply', id='stats-cust-apply',
                         classes='stats-apply-btn')

        # ── chart area ────────────────────────────────────────────────────────
        with Vertical(id='stats-chart-area'):
            with Horizontal(id='stats-chart-header'):
                yield Static('', id='stats-chart-title', markup=True)
                yield Static('', id='stats-y-max', markup=True)
            with Horizontal(id='stats-chart-body'):
                yield Static('', id='stats-y-axis')
                with Vertical(id='stats-chart-inner'):
                    yield Sparkline([], id='stats-sparkline', summary_function=max)
                    yield Static('', id='stats-user-chart')
                    yield Static('', id='stats-user-legend')
                    yield Static('', id='stats-x-axis')
                    yield Static('', id='stats-hover-info', markup=True)

        # ── summary bar ───────────────────────────────────────────────────────
        with Horizontal(id='stats-summary'):
            yield Label('', id='stats-lbl-total',   markup=True)
            yield Label('', id='stats-lbl-peak',    markup=True)
            yield Label('', id='stats-lbl-jobs',    markup=True)
            yield Label('', id='stats-lbl-loading', markup=True)

        # ── per-group sparklines (scrollable) ────────────────────────────────
        with VerticalScroll(id='stats-breakdown-scroll'):
            pass  # populated dynamically

        # ── issue stats (chart + table) ───────────────────────────────────────
        with Vertical(id='stats-issues'):
            yield Static('', id='stats-issue-chart', markup=True)
            yield Static('', id='stats-issue-table', markup=True)

    def on_mount(self) -> None:
        self._w_sparkline = self.query_one(_SPARKLINE_ID, Sparkline)
        self._w_hover     = self.query_one(_HOVER_INFO_ID, Static)
        self._update_dim_btns()
        self.query_one(_FILTER_BAR).display = False
        self.query_one('#stats-custom-bar').display = False
        self.query_one(_ISSUES_ID).display = False
        self.query_one(_STACKED_ID).display = False
        self.query_one(_LEGEND_ID).display = False
        self._load()
        self._load_issues()

    # ── Button / select handlers ───────────────────────────────────────────────

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ''
        if bid.startswith('stats-dim-'):
            self._set_dim(bid[len('stats-dim-'):])
        elif bid == 'stats-refresh-btn':
            self.action_refresh_data()
        elif bid == 'stats-cust-apply':
            self._apply_custom()

    def on_select_changed(self, event: Select.Changed) -> None:
        sel_id = event.select.id or ''
        if sel_id == 'stats-range-select':
            rk = str(event.value) if event.value is not None else '7d'
            if rk != 'custom':
                self._custom_start = self._custom_end = ''
            self._set_range(rk)
        elif sel_id.startswith('stats-filter'):
            v = event.value
            self._filter_val = '' if (v is Select.BLANK or str(v) == '—') else str(v)
            self._load()

    # ── Dim / range state ──────────────────────────────────────────────────────

    def _set_dim(self, dim: str) -> None:
        self._dim = dim
        self._update_dim_btns()
        is_issues = dim == 'issues'
        # show/hide the two content areas
        for wid in ('#stats-chart-area', '#stats-summary', '#stats-breakdown'):
            try:
                self.query_one(wid).display = not is_issues
            except Exception:
                pass
        self.query_one(_ISSUES_ID).display = is_issues
        if is_issues:
            self.query_one(_FILTER_BAR).display = False
            self._load_issues()
            return
        is_filter = dim != 'cluster'
        self.query_one(_FILTER_BAR).display = is_filter
        if is_filter:
            self._populate_filter()
        else:
            self._filter_val = ''
            self._load()

    def _set_range(self, rk: str) -> None:
        self._range_key = rk
        is_custom = rk == 'custom'
        self.query_one('#stats-custom-bar').display = is_custom
        if self._dim == 'issues':
            self._load_issues()
            return
        if is_custom:
            # Move keyboard focus to the From input so the user can type immediately
            try:
                self.query_one('#stats-cust-start', Input).focus()
            except Exception:
                pass
        else:
            self._load()

    def _update_dim_btns(self) -> None:
        for d in list(_DIMS) + ['issues']:
            try:
                self.query_one(f'#stats-dim-{d}', Button).set_class(
                    d == self._dim, '--active')
            except Exception:
                pass

    # ── Filter population ──────────────────────────────────────────────────────

    def _populate_filter(self) -> None:
        dim = self._dim

        sinfo_ok = getattr(self.app, '_cmd_sinfo', True)

        def _worker() -> List[Tuple[str, str]]:
            from speek.speek_max.slurm import (
                get_partitions, parse_nodes, fetch_filter_users,
            )
            if dim == 'partition':
                return [(p, p) for p in get_partitions()] if sinfo_ok else []
            if dim == 'node':
                return [(r[0], r[0]) for r in parse_nodes()]
            if dim == 'user':
                return [(u, u) for u in fetch_filter_users()]
            return []

        self.run_worker(_worker, thread=True, group='stats-filter')

    # ── Time window ────────────────────────────────────────────────────────────

    def _get_window(self) -> Tuple[datetime, datetime]:
        now = datetime.now()
        if self._range_key == 'custom' and self._custom_start and self._custom_end:
            try:
                s = datetime.strptime(self._custom_start, '%Y-%m-%d')
                e = datetime.strptime(self._custom_end,   '%Y-%m-%d').replace(
                    hour=23, minute=59, second=59)
                return s, e
            except ValueError:
                pass
        delta, _, _ = _RANGES.get(self._range_key, _RANGES['7d'])
        return now - delta, now

    def _apply_custom(self) -> None:
        try:
            self._custom_start = self.query_one(
                '#stats-cust-start', Input).value.strip()
            self._custom_end   = self.query_one(
                '#stats-cust-end',   Input).value.strip()
            if self._custom_start and self._custom_end:
                self._load()
        except Exception:
            pass

    # ── Data load ──────────────────────────────────────────────────────────────

    def action_refresh_data(self) -> None:
        self._load()
        self._load_issues()

    def _load_issues(self) -> None:
        if not getattr(self.app, '_cmd_sacct', True):
            return
        if not getattr(self.app, '_feat_issue_stats', True):
            return
        # Use the same time window as the stats chart (respects custom range)
        start, end = self._get_window()
        delta = end - start
        hours = max(1, int(delta.total_seconds() / 3600))

        # Check issue-level cache (reuse if same hours and < 60s old)
        import time as _time
        _issue_cache = getattr(self, '_issue_result_cache', None)
        if _issue_cache is not None:
            c_hours, c_ts, c_data = _issue_cache
            if c_hours == hours and (_time.monotonic() - c_ts) < 60.0:
                self._render_issues(c_data, {}, hours)
                return

        def _worker():
            try:
                from speek.speek_max.slurm import fetch_issue_stats, fetch_history, get_job_log_path
                from speek.speek_max.log_scan import detect_oom
                from collections import defaultdict
                import time as _t

                data = fetch_issue_stats(hours)

                # Log OOM scan — use persistent verdict cache + scan new jobs
                days = max(1, hours // 24) or 1
                cache = getattr(self, '_oom_log_cache', {})
                cached = cache.get(days)
                now = _t.monotonic()
                if cached and now - cached[0] < 60.0:
                    oom_by_model, oom_by_node = cached[1], cached[2]
                else:
                    oom_by_model = defaultdict(int)
                    oom_by_node = defaultdict(int)
                    try:
                        # Load persistent OOM verdicts from Events widget cache
                        import json as _json
                        from pathlib import Path as _Path
                        import os as _os
                        verdict_file = _Path(
                            _os.environ.get('XDG_CACHE_HOME', _Path.home() / '.cache')
                        ) / 'speek' / 'oom_verdicts.json'
                        oom_verdicts = {}
                        try:
                            oom_verdicts = _json.loads(verdict_file.read_text())
                        except Exception:
                            pass
                        known_oom_jids = {jid for jid, v in oom_verdicts.items() if v}

                        rows = fetch_history(days=days)
                        scanned = 0
                        for r in rows:
                            jid, part = r[0], r[2]
                            state = r[5].strip() if len(r) > 5 else ''
                            nodelist = r[8].strip() if len(r) > 8 else ''
                            is_oom = False
                            # Check verdict cache first (instant)
                            if jid in known_oom_jids:
                                is_oom = True
                            elif jid in oom_verdicts:
                                pass  # already scanned, not OOM
                            elif state in ('COMPLETED', 'RUNNING') and scanned < 50:
                                # Scan log for new jobs
                                scanned += 1
                                path = get_job_log_path(jid)
                                if path and detect_oom(path):
                                    is_oom = True
                            # SLURM-reported OOM state
                            if state == 'OUT_OF_MEMORY':
                                is_oom = True
                            if is_oom:
                                oom_by_model[part] += 1
                                for nd in nodelist.split(','):
                                    nd = nd.strip()
                                    if nd and nd not in ('None', ''):
                                        oom_by_node[nd] += 1
                    except Exception:
                        pass
                    cache[days] = (now, dict(oom_by_model), dict(oom_by_node))
                    self._oom_log_cache = cache

                # Merge log-detected OOM into issue stats
                if oom_by_model:
                    by_model = data.setdefault('by_model', {})
                    for model, count in oom_by_model.items():
                        if model not in by_model:
                            by_model[model] = {'failed': 0, 'timeout': 0, 'oom': 0, 'total': 0}
                        by_model[model]['oom_log'] = count
                        by_model[model]['total'] += count
                    by_node = data.setdefault('by_node', {})
                    for node, count in oom_by_node.items():
                        if node not in by_node:
                            by_node[node] = {'failed': 0, 'timeout': 0, 'oom': 0, 'total': 0}
                        by_node[node]['oom_log'] = count
                        by_node[node]['total'] += count

                import time as _t2
                self._issue_result_cache = (hours, _t2.monotonic(), data)
                self.app.call_from_thread(self._render_issues, data, {}, hours)
            except Exception:
                pass

        self.run_worker(_worker, thread=True, exclusive=True, group='stats-issues')

    def _load(self) -> None:
        try:
            self.query_one(_LOADING_ID, Label).update('[dim]loading…[/dim]')
        except Exception:
            pass

        start, end   = self._get_window()
        _, n_buckets, _ = _RANGES.get(self._range_key, _RANGES['7d'])
        dim   = self._dim
        fval  = self._filter_val

        def _worker():
            try:
                from speek.speek_max.slurm import (
                    fetch_stats_rows_chunked, _compute_timeseries, _compute_breakdown,
                    _compute_per_group_timeseries,
                )

                def on_chunk(rows, done: int, total: int) -> None:
                    # Main chart always shows cluster-wide total (same as Cluster tab)
                    ts = _compute_timeseries(rows, start, end, 'cluster', '', n_buckets)
                    bd = _compute_breakdown(rows, dim)
                    pg = _compute_per_group_timeseries(rows, start, end, dim, n_buckets) if done == total else {}
                    lbl = f'[dim]{done}/{total} days…[/dim]' if done < total else ''
                    self.app.call_from_thread(self._render_partial, ts, bd, lbl, pg)

                fetch_stats_rows_chunked(start, end, on_chunk)
            except Exception as exc:
                self.app.call_from_thread(
                    lambda: self.query_one(_LOADING_ID, Label).update(
                        f'[bold red]error: {exc}[/bold red]'
                    )
                )

        self.run_worker(_worker, thread=True, exclusive=True, group='stats-load')

    @safe('Stats render')
    def _render_partial(self, ts: Dict, bd: List[Dict], loading_label: str = '',
                        per_group: Dict = None) -> None:
        """Called from worker thread via call_from_thread for each day chunk."""
        self._render_stats(ts, bd, per_group)
        if not loading_label:
            # Final chunk: show "no data" if sacct returned nothing
            if ts.get('n_jobs', 0) == 0 and not ts.get('buckets'):
                loading_label = '[dim]no data — sacct returned nothing for this window[/dim]'
        try:
            self.query_one(_LOADING_ID, Label).update(loading_label)
        except Exception:
            pass

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.state != WorkerState.SUCCESS:
            return
        grp = event.worker.group
        if grp == 'stats-filter':
            opts: List[Tuple[str, str]] = event.worker.result
            try:
                sel = self.query_one('#stats-filter-sel', SpeekSelect)
                sel.set_options(opts)
                if opts:
                    first = str(opts[0][1])
                    if first != self._filter_val:
                        self._filter_val = first
                    self._load()
            except Exception:
                pass
        # 'stats-load' renders progressively via call_from_thread; no action needed here.

    # ── Render ─────────────────────────────────────────────────────────────────

    def _render_stats(self, ts: Dict, bd: List[Dict], per_group: Dict = None) -> None:
        # chart title
        try:
            rk   = self._range_key.upper() if self._range_key != 'custom' else 'Custom'
            dim  = _DIM_LABELS.get(self._dim, self._dim.capitalize())
            filt = f'  [dim]{self._filter_val}[/dim]' if self._filter_val else ''
            self.query_one('#stats-chart-title', Static).update(
                f'[bold]GPU Usage[/bold] — {dim}{filt}  [dim]{rk}[/dim]')
        except Exception:
            pass

        # store for hover lookup
        self._ts_buckets = list(ts.get('buckets', []))
        self._ts_labels  = list(ts.get('labels',  []))

        # sparkline + y-max label
        is_user_dim = self._dim == 'user' and per_group
        try:
            from io import StringIO
            from rich.console import Console as _RCon

            if is_user_dim:
                # Show stacked colored chart, hide sparkline
                sparkline = self.query_one(_SPARKLINE_ID, Sparkline)
                sparkline.display = False
                peak = ts.get('peak', 0)
                try:
                    chart_w = sparkline.size.width or 60
                except Exception:
                    chart_w = 60
                chart_text = _render_stacked_chart(per_group, peak, chart_w)
                stacked = self.query_one(_STACKED_ID, Static)
                stacked.update(chart_text)
                stacked.display = True
                try:
                    self.query_one(_LEGEND_ID).display = False
                except Exception:
                    pass
            else:
                # Show sparkline, hide stacked chart
                self.query_one(_SPARKLINE_ID, Sparkline).data = ts['buckets']
                self.query_one(_SPARKLINE_ID, Sparkline).display = True
                try:
                    self.query_one(_STACKED_ID).display = False
                    self.query_one(_LEGEND_ID).display = False
                except Exception:
                    pass

            peak = ts.get('peak', 0)
            self._last_peak = peak
            self.query_one('#stats-y-max', Static).update(
                f'[dim]▲ {peak:.0f} GPUs[/dim]' if peak else '')
            self.call_after_refresh(self._update_y_axis)
        except Exception:
            pass

        # x-axis labels are redrawn on resize via _update_x_axis
        self.call_after_refresh(self._update_x_axis)

        # summary
        try:
            total = ts['total_gpu_hours']
            peak  = ts['peak']
            jobs  = ts['n_jobs']
            self.query_one('#stats-lbl-total', Label).update(
                f'[dim]Total[/dim] [bold green]{total:.1f}[/bold green] GPU·h  ')
            self.query_one('#stats-lbl-peak',  Label).update(
                f'[dim]Peak[/dim] [bold]{peak:.0f}[/bold] GPUs  ')
            self.query_one('#stats-lbl-jobs',  Label).update(
                f'[dim]Jobs[/dim] [bold]{jobs}[/bold]')
        except Exception:
            pass

        # per-group sparklines in scrollable area
        if per_group:
            try:
                scroll = self.query_one('#stats-breakdown-scroll', VerticalScroll)
                scroll.remove_children()
                # Sort by total GPU hours descending
                sorted_groups = sorted(
                    per_group.items(),
                    key=lambda kv: sum(kv[1].get('buckets', [])),
                    reverse=True,
                )
                # Build color map for user dimension
                user_color_map: Dict[str, str] = {}
                if self._dim == 'user':
                    user_color_map = {
                        u: _USER_COLORS[i % len(_USER_COLORS)]
                        for i, (u, _) in enumerate(sorted_groups)
                    }
                for grp_name, grp_ts in sorted_groups:
                    buckets = grp_ts.get('buckets', [])
                    peak = grp_ts.get('peak', 0)
                    total_h = grp_ts.get('total_gpu_hours', 0)
                    jobs = grp_ts.get('n_jobs', 0)
                    # Color indicator for user dimension
                    color = user_color_map.get(grp_name, '')
                    name_prefix = f'[{color}]●[/{color}] ' if color else ''
                    # Container for each group
                    container = Vertical(classes='stats-group-row')
                    header = Static(
                        f'{name_prefix}[bold]{grp_name}[/bold]  '
                        f'[dim]peak[/dim] [bold]{peak:.0f}[/bold]  '
                        f'[dim]{total_h:.1f} GPU·h[/dim]  '
                        f'[dim]{jobs} jobs[/dim]',
                        markup=True,
                    )
                    spark = Sparkline(buckets, summary_function=max)
                    scroll.mount(container)
                    container.mount(header)
                    container.mount(spark)
                    # Apply per-user color to sparkline
                    if color:
                        spark.styles.color = color
            except Exception:
                pass

    def _update_y_axis(self) -> None:
        """Re-render y-axis ticks based on current sparkline height."""
        try:
            peak = getattr(self, '_last_peak', 0)
            h = self.query_one(_SPARKLINE_ID, Sparkline).size.height
            self.query_one('#stats-y-axis', Static).update(
                _build_y_axis(peak, h) if (h > 0 and peak > 0) else ''
            )
        except Exception:
            pass

    def _update_x_axis(self) -> None:
        """Re-render x-axis labels to fill the available width.

        Labels are positioned proportionally so that e.g. '7d' and '1d'
        are visually at the correct distance from each other.
        """
        try:
            labels = self._ts_labels
            if not labels:
                return
            axis = self.query_one('#stats-x-axis', Static)
            w = axis.size.width
            if w <= 0:
                return
            n = len(labels)
            # Pick ~max_labels evenly spaced indices
            avg_len = max(len(labels[0]), 4) + 2 if labels else 8
            max_labels = max(2, w // avg_len)
            step = max(1, n // max_labels)
            indices = list(range(0, n, step))
            # Build a fixed-width line with labels at proportional positions
            buf = [' '] * w
            for idx in indices:
                lbl = labels[idx]
                pos = int(idx / n * w)
                pos = min(pos, w - len(lbl))
                pos = max(pos, 0)
                for j, ch in enumerate(lbl):
                    if pos + j < w:
                        buf[pos + j] = ch
            axis.update(''.join(buf))
        except Exception:
            pass

    def on_resize(self) -> None:
        self._update_y_axis()
        self._update_x_axis()

    # ── Sparkline hover ────────────────────────────────────────────────────────

    def on_mouse_move(self, event) -> None:
        info = self._w_hover
        sl   = self._w_sparkline
        if info is None or sl is None or not self._ts_buckets:
            return
        region = sl.region
        n      = len(self._ts_buckets)
        rel_x  = event.screen_x - region.x
        if region.width == 0 or not (0 <= rel_x < region.width):
            if self._last_hover_idx != -1:
                self._last_hover_idx = -1
                info.update('')
                info.styles.offset = (0, 0)
            return
        idx = min(int(rel_x * n / region.width), n - 1)
        if idx == self._last_hover_idx:
            return  # same bucket — nothing to do
        self._last_hover_idx = idx
        val = self._ts_buckets[idx]
        lbl = self._ts_labels[idx] if idx < len(self._ts_labels) else ''
        info.update(
            f'  [dim]{lbl}[/dim]  [bold]{val:.1f}[/bold] GPUs  '
            if lbl else f'  [bold]{val:.1f}[/bold] GPUs  '
        )
        # Float the label near the cursor
        info_region = info.region
        ox = event.screen_x - info_region.x
        oy = event.screen_y - info_region.y - 1
        info.styles.offset = (ox, oy)

    def on_leave(self, _event) -> None:
        self._last_hover_idx = -1
        if self._w_hover is not None:
            self._w_hover.update('')
            self._w_hover.styles.offset = (0, 0)

    @safe('Stats issues')
    def _render_issues(self, data: Dict, ts_data: Dict, hours: int) -> None:
        try:
            self.query_one('#stats-issue-chart', Static).update(
                _build_issues(data, hours))
            self.query_one('#stats-issue-table', Static).update('')
        except Exception:
            pass
