"""stats_widget.py — GPU time-usage statistics: sparkline timeline + breakdown table."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from rich.text import Text
from rich.table import Table
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widget import Widget
from textual.widgets import Button, Input, Label, Select, Sparkline, Static

from speek.speek_max.widgets.select import SpeekSelect
from speek.speek_max._utils import safe

_LOADING_ID  = '#stats-lbl-loading'
_FILTER_BAR  = '#stats-filter-bar'
_ISSUES_ID   = '#stats-issues'
_SPARKLINE_ID  = '#stats-sparkline'
_HOVER_INFO_ID = '#stats-hover-info'

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
        t.add_row(r['name'][:22], f"{r['gpu_hours']:.1f}", str(r['jobs']), bar)
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
    t.add_column('', min_width=12, no_wrap=True)  # name
    t.add_column('', justify='right', min_width=4)  # F
    t.add_column('', justify='right', min_width=4)  # T
    t.add_column('', justify='right', min_width=4)  # O
    t.add_column('', min_width=BAR_W)  # bar

    if not by_model and not by_node:
        t.add_row(
            Text(f'No issues in the last {hours}h', style='dim green'),
            Text(''), Text(''), Text(''), Text('✓', style='bold green'),
        )
        return t

    # Totals
    total_f = sum(d.get('failed', 0) for d in by_model.values())
    total_t = sum(d.get('timeout', 0) for d in by_model.values())
    total_o = sum(d.get('oom', 0) for d in by_model.values())
    total_all = total_f + total_t + total_o

    # Summary header
    summary = Text()
    summary.append(f'{total_all}', style='bold red' if total_all else 'bold green')
    summary.append(f' issues ', style='dim')
    summary.append(f'(last {hours}h)', style='dim')
    t.add_row(summary, Text(''), Text(''), Text(''), Text(''))

    # Column labels
    t.add_row(
        Text('Partition', style='bold'),
        Text('F', style='bold red'),
        Text('T', style='bold yellow'),
        Text('O', style='bold #ff00ff'),
        Text('', style='dim'),
    )

    max_total = max((d.get('total', 0) for d in by_model.values()), default=1) or 1
    for name in sorted(by_model, key=lambda k: by_model[k].get('total', 0), reverse=True):
        d = by_model[name]
        f, to, o, tot = d.get('failed', 0), d.get('timeout', 0), d.get('oom', 0), d.get('total', 0)
        # Stacked bar: red=failed, yellow=timeout, magenta=OOM
        bar = Text()
        w_total = max(1, int(round(tot / max_total * BAR_W)))
        w_f = int(round(f / tot * w_total)) if tot else 0
        w_o = int(round(o / tot * w_total)) if tot else 0
        w_t = w_total - w_f - w_o
        if w_f > 0: bar.append('█' * w_f, style='red')
        if w_t > 0: bar.append('█' * w_t, style='yellow')
        if w_o > 0: bar.append('█' * w_o, style='#ff00ff')
        bar.append('░' * (BAR_W - w_total), style='dim')
        t.add_row(
            Text(name[:14], style='bold'),
            Text(str(f), style=_issue_color(f)) if f else Text('·', style='dim'),
            Text(str(to), style=_issue_color(to)) if to else Text('·', style='dim'),
            Text(str(o), style=_issue_color(o)) if o else Text('·', style='dim'),
            bar,
        )

    if by_node:
        t.add_row(Text(''), Text(''), Text(''), Text(''), Text(''))
        t.add_row(
            Text('Node', style='bold'),
            Text('F', style='bold red'),
            Text('T', style='bold yellow'),
            Text('O', style='bold #ff00ff'),
            Text('', style='dim'),
        )
        max_node = max((d.get('total', 0) for d in by_node.values()), default=1) or 1
        for name in sorted(by_node, key=lambda k: by_node[k].get('total', 0), reverse=True)[:8]:
            d = by_node[name]
            f, to, o, tot = d.get('failed', 0), d.get('timeout', 0), d.get('oom', 0), d.get('total', 0)
            bar = Text()
            w_total = max(1, int(round(tot / max_node * BAR_W)))
            w_f = int(round(f / tot * w_total)) if tot else 0
            w_o = int(round(o / tot * w_total)) if tot else 0
            w_t = w_total - w_f - w_o
            if w_f > 0: bar.append('█' * w_f, style='red')
            if w_t > 0: bar.append('█' * w_t, style='yellow')
            if w_o > 0: bar.append('█' * w_o, style='#ff00ff')
            bar.append('░' * (BAR_W - w_total), style='dim')
            t.add_row(
                Text(name[:14]),
                Text(str(f), style=_issue_color(f)) if f else Text('·', style='dim'),
                Text(str(to), style=_issue_color(to)) if to else Text('·', style='dim'),
                Text(str(o), style=_issue_color(o)) if o else Text('·', style='dim'),
                bar,
            )

    # Legend
    t.add_row(Text(''), Text(''), Text(''), Text(''), Text(''))
    legend = Text()
    legend.append('█', style='red')
    legend.append(' Failed  ', style='dim')
    legend.append('█', style='yellow')
    legend.append(' Timeout  ', style='dim')
    legend.append('█', style='#ff00ff')
    legend.append(' OOM', style='dim')
    t.add_row(Text(''), Text(''), Text(''), Text(''), legend)

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
                    yield Static('', id='stats-x-axis')
                    yield Static('', id='stats-hover-info', markup=True)

        # ── summary bar ───────────────────────────────────────────────────────
        with Horizontal(id='stats-summary'):
            yield Label('', id='stats-lbl-total',   markup=True)
            yield Label('', id='stats-lbl-peak',    markup=True)
            yield Label('', id='stats-lbl-jobs',    markup=True)
            yield Label('', id='stats-lbl-loading', markup=True)

        # ── breakdown table ───────────────────────────────────────────────────
        yield Static('', id='stats-breakdown', markup=True)

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
        hours = getattr(self.app, '_issue_hours', 24)

        def _worker():
            try:
                from speek.speek_max.slurm import fetch_issue_stats
                data = fetch_issue_stats(hours)
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
                )

                def on_chunk(rows, done: int, total: int) -> None:
                    ts = _compute_timeseries(rows, start, end, dim, fval, n_buckets)
                    bd = _compute_breakdown(rows, dim)
                    lbl = f'[dim]{done}/{total} days…[/dim]' if done < total else ''
                    self.app.call_from_thread(self._render_partial, ts, bd, lbl)

                fetch_stats_rows_chunked(start, end, on_chunk)
            except Exception as exc:
                self.app.call_from_thread(
                    lambda: self.query_one(_LOADING_ID, Label).update(
                        f'[bold red]error: {exc}[/bold red]'
                    )
                )

        self.run_worker(_worker, thread=True, exclusive=True, group='stats-load')

    @safe('Stats render')
    def _render_partial(self, ts: Dict, bd: List[Dict], loading_label: str = '') -> None:
        """Called from worker thread via call_from_thread for each day chunk."""
        self._render_stats(ts, bd)
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

    def _render_stats(self, ts: Dict, bd: List[Dict]) -> None:
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
        try:
            self.query_one(_SPARKLINE_ID, Sparkline).data = ts['buckets']
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

        # breakdown
        try:
            self.query_one('#stats-breakdown', Static).update(
                _build_breakdown(bd, self._dim))
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
