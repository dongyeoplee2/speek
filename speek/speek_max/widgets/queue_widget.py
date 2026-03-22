"""queue_widget.py — Full cluster queue panel."""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.widget import Widget
from textual.widgets import Label, LoadingIndicator, Static

from speek.speek_max.slurm import fetch_all_priorities, fetch_job_details, fetch_job_stats, fetch_queue
from speek.speek_max._utils import fmt_time, tc, safe, state_sym
from speek.speek_max.widgets.datatable import SpeekDataTable
from speek.speek_max.widgets.foldable_table import (
    FoldableTableMixin, FoldGroup, FoldMode, Leaf, Divider, Spacer,
    TreeNode, TableContext, _build_divider_cells,
)
from speek.speek_max.widgets.ping_tracker import PingTracker


def _color_flash(cell: Text, intensity: float) -> Text:
    """Flash a changed cell's text to bright white, fading back to original color."""
    if intensity <= 0:
        return cell
    # Blend towards white based on intensity
    bright = int(180 + intensity * 75)  # 180–255 range
    flash_color = f'#{bright:02x}{bright:02x}{bright:02x}'
    return Text(cell.plain, style=f'bold {flash_color}')


_RANK_EMOJI = {1: '🥇', 2: '🥈', 3: '🥉'}


_TRAIL_RE = re.compile(r'[\d_\-\.]+$')

def _name_base(name: str) -> str:
    """Strip trailing numbers/separators: 'train_run_007' → 'train_run_'"""
    return _TRAIL_RE.sub('', name)

def _should_merge(a: str, b: str) -> bool:
    """True if two job names are similar enough to group together."""
    if a == b:
        return True
    ba, bb = _name_base(a), _name_base(b)
    return bool(ba and ba == bb)


def _pre_build_queue_rows(
    rows: List[Tuple], priorities: Optional[Dict], tv: dict,
) -> Tuple[frozenset, List[Tuple], Optional['Text'], bool]:
    """Aggregate + pre-build Rich Text rows for the queue DataTable.
    Runs in a worker thread so the main thread only does DataTable DOM ops.

    Returns (sig, built, stats_text, has_rows).
    Each entry in built is (col0..col8(9 cols), row_key, partition_name, jobs).
    """
    c_muted     = tc(tv, 'text-muted',     'bright_black')
    c_primary   = tc(tv, 'text-primary',   'bold')
    c_secondary = tc(tv, 'text-secondary', 'default')
    c_warning   = tc(tv, 'text-warning',   'yellow')

    # Structural signature for skip-rebuild check (excludes elapsed)
    sig = frozenset((r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows)

    # Aggregation
    groups: Dict[tuple, dict] = {}
    for jid, user, name, part, gpus, state, elapsed, start in rows:
        matched = None
        for key, g in groups.items():
            ku, kn, kp, ks = key
            if ku == user and kp == part and ks == state and _should_merge(kn, name):
                matched = key
                break
        if matched is None:
            matched = (user, name, part, state)
            groups[matched] = {'ids': [], 'gpus': 0, 'elapsed': elapsed,
                               'first_id': jid, 'name': name, 'start': start}
        g = groups[matched]
        g['ids'].append(jid)
        g.setdefault('jobs', []).append({
            'jid': jid, 'user': user, 'name': name,
            'part': part, 'gpus': gpus, 'state': state, 'elapsed': elapsed,
            'start': start,
        })
        try:
            g['gpus'] += int(gpus)
        except (ValueError, TypeError):
            pass
        if elapsed > g['elapsed']:
            g['elapsed'] = elapsed
        # Keep earliest start time
        if start and (not g['start'] or start < g['start']):
            g['start'] = start

    # Top-3 user rank by running GPUs
    user_running: Dict[str, int] = defaultdict(int)
    for (user, _, _, state), g in groups.items():
        if state == 'RUNNING':
            user_running[user] += g['gpus']
    ranked = [u for u, _ in sorted(user_running.items(), key=lambda x: -x[1]) if _ > 0]
    user_rank = {u: i + 1 for i, u in enumerate(ranked[:3])}

    # Sort groups: partition first, then RUNNING before PENDING, then by user
    _state_order = {'RUNNING': 0, 'PENDING': 1}
    sorted_keys = sorted(
        groups.keys(),
        key=lambda k: (k[2], _state_order.get(k[3], 2), k[0]),  # (partition, state_rank, user)
    )

    # Pre-build Rich Text row tuples: (col0, col1, …, col8, row_key, partition)
    built: List[Tuple] = []
    for key in sorted_keys:
        user, _kn, part, state = key
        g = groups[key]
        count   = len(g['ids'])
        gpu_str = str(g['gpus']) if g['gpus'] else '-'
        emoji   = _RANK_EMOJI.get(user_rank.get(user, 0), '')
        user_cell = Text()
        user_cell.append(emoji)
        user_cell.append(user, style=f'bold {c_primary}')
        prio_text = Text('-', style=c_muted)
        if state == 'PENDING' and priorities:
            entry = priorities.get(g['first_id'])
            if entry:
                try:
                    prio_text = Text(str(int(float(entry.get('total', 0)))),
                                     style=c_warning)
                except (ValueError, TypeError):
                    pass
        ago_str = fmt_time(g['start']) if g.get('start') and g['start'] not in ('N/A', 'Unknown') else '—'
        built.append((
            Text(f'   {g["name"]}'),
            Text(str(count), style=f'bold {c_muted}'),
            user_cell,
            Text(gpu_str, style='bold'),
            _state_text(state, tv),
            Text(g['elapsed'], style=c_muted),
            Text(ago_str, style=c_muted),
            prio_text,
            Text(_fmt_job_ids(g['ids']), style=c_muted),
            g['first_id'],   # row key — NOT a column cell
            part,            # partition name — NOT a column cell
            g['jobs'],       # raw per-job data — NOT a column cell
        ))

    # Pre-build stats bar text
    stats_text: Optional[Text] = None

    return sig, built, stats_text, bool(rows)


def _fmt_job_ids(ids: List[str], max_len: int = 32) -> str:
    """Compact range notation: 1234-1238, 1241 (truncated to max_len chars)."""
    try:
        ints = sorted(int(i) for i in ids)
    except ValueError:
        return ', '.join(ids[:4])
    groups, start, prev = [], ints[0], ints[0]
    for x in ints[1:]:
        if x == prev + 1:
            prev = x
        else:
            groups.append((start, prev))
            start = prev = x
    groups.append((start, prev))
    parts = [str(a) if a == b else f'{a}-{b}' for a, b in groups]
    result, buf = '', ''
    for i, p in enumerate(parts):
        buf = (result + ', ' + p) if result else p
        if len(buf) > max_len:
            remaining = len(parts) - i
            result += f' +{remaining}…'
            break
        result = buf
    return result


def _state_text(state: str, tv: dict) -> Text:
    from speek.speek_max._utils import state_badge
    return state_badge(state)


class QueueWidget(FoldableTableMixin, Widget):
    """Full cluster queue (all users, RUNNING+PENDING). Auto-refreshes every 5s."""

    BORDER_TITLE = "Queue"
    can_focus = True

    BINDINGS = [
        Binding('d', 'job_detail', 'Detail', show=True),
        Binding('r', 'refresh', 'Refresh', show=True),
        Binding('v', 'toggle_fold', '▶/▼', show=True),
        Binding('V', 'fold_all', '', show=False),
    ]

    def compose(self) -> ComposeResult:
        """Compose the queue widget."""
        yield LoadingIndicator()
        yield Static('', id='queue-empty', classes='empty-state')
        yield SpeekDataTable(id='queue-dt', cursor_type='row', show_cursor=True)
        yield Static('', id='queue-stats')

    def on_mount(self) -> None:
        self._last_queue_sig: frozenset = frozenset()
        self._last_built: List[Tuple] = []
        self._last_stats_text: Optional[Text] = None
        self._last_has_rows: bool = False
        self._ctx = self._init_ctx(renderer=self._render_cell, n_cols=9, name_col_width=22)
        ping_dur = getattr(self.app, '_ping_duration', 10)
        self._ping = PingTracker(duration=ping_dur)
        self._tree: List[TreeNode] = []
        dt = self.query_one(SpeekDataTable)
        self._col_widths = [20, 3, 12, 5, 9, 9, 5, 6, 24]
        dt.zebra_stripes = True
        dt.add_column('Name',      width=22)
        dt.add_column('#J',         width=2)
        dt.add_column('User',      width=8)
        dt.add_column('#G',       width=3)
        dt.add_column('State',     width=3)
        dt.add_column('Elapsed',   width=7)
        dt.add_column('Ago',       width=5)
        dt.add_column('Prio',      width=4)
        dt.add_column('IDs',       width=16)

    def on_show(self) -> None:
        self._load()
        if not hasattr(self, '_interval_started'):
            self._interval_started = True
            interval = getattr(self.app, '_queue_refresh', 5)
            self._refresh_timer = self.set_interval(interval, self._load)

    def set_refresh_interval(self, seconds: int) -> None:
        try:
            self._refresh_timer.stop()
        except Exception:
            pass
        self._refresh_timer = self.set_interval(seconds, self._load)

    def on_click(self, event) -> None:
        try:
            self.query_one(SpeekDataTable).focus()
        except Exception:
            pass

    def _load(self) -> None:
        if not getattr(self.app, '_cmd_squeue', True):
            try:
                self.query_one(LoadingIndicator).display = False
                e = self.query_one('#queue-empty', Static)
                e.update('[dim]squeue unavailable on this cluster[/dim]')
                e.display = True
            except Exception:
                pass
            return
        tv = self.app.theme_variables  # snapshot on main thread before worker starts
        self.run_worker(
            lambda: self._fetch(tv), thread=True, exclusive=True, group='queue',
        )

    def _fetch(self, tv: dict) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        rows       = fetch_queue()
        job_stats  = fetch_job_stats()
        priorities = fetch_all_priorities()
        if worker.is_cancelled:
            return
        sig, built, _, has_rows = _pre_build_queue_rows(rows, priorities, tv)
        # Pre-build stats bar text in the worker too
        c_muted   = tc(tv, 'text-muted',   'bright_black')
        c_primary = tc(tv, 'text-primary', 'bold')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        stats_text = None
        if job_stats:
            t = Text()
            t.append(str(job_stats.get('total_R',  0)), style=f'bold {c_success}')
            t.append(' running  ',                      style=c_muted)
            t.append(str(job_stats.get('total_PD', 0)), style=f'bold {c_warning}')
            t.append(' pending  ',                      style=c_muted)
            t.append(str(job_stats.get('active_users', 0)), style=f'bold {c_primary}')
            t.append(' active users',                   style=c_muted)
            stats_text = t
        self.app.call_from_thread(self._update, sig, built, stats_text, has_rows)

    @safe('Queue update')
    def _update(self, sig: frozenset, built: List[Tuple],
                stats_text: Optional[Text], has_rows: bool) -> None:
        if sig == self._last_queue_sig:
            return
        self._last_queue_sig = sig
        self._last_built = built
        self._last_stats_text = stats_text
        self._last_has_rows = has_rows

        # Update ping tracker with per-cell signatures (job rows only)
        self._ping.duration = getattr(self.app, '_ping_duration', 10)
        row_sigs: dict[str, list[str]] = {}
        for *cells, row_key, _part, _jobs in built:
            row_sigs[str(row_key)] = [
                c.plain if hasattr(c, 'plain') else str(c) for c in cells
            ]
        self._ping.update(row_sigs)

        self._rebuild_table(stats_text, has_rows)

    # ── Tree building ────────────────────────────────────────────────────────

    def _build_tree(self, built: List[Tuple]) -> List[TreeNode]:
        """Convert pre-built rows into a tree of partition dividers and job groups."""
        # Group built rows by partition, preserving sort order
        partition_order: List[str] = []
        partition_rows: Dict[str, List[Tuple]] = defaultdict(list)
        partition_gpus: Dict[str, int] = defaultdict(int)
        partition_count: Dict[str, int] = defaultdict(int)
        for entry in built:
            *cells, row_key, part, jobs = entry
            if part not in partition_rows:
                partition_order.append(part)
            partition_rows[part].append(entry)
            try:
                partition_gpus[part] += int(cells[3].plain) if cells[3].plain != '-' else 0
            except (ValueError, TypeError):
                pass
            partition_count[part] += int(cells[1].plain) if cells[1].plain else 1

        # Sort partitions to match cluster bar order + get usage for coloring
        from speek.speek_max.slurm import fetch_cluster_stats
        part_usage: Dict[str, float] = {}
        try:
            cstats = fetch_cluster_stats()
            order = sorted(cstats, key=lambda m: cstats[m]['Total'], reverse=True)
            rank = {m: i for i, m in enumerate(order)}
            partition_order.sort(key=lambda p: rank.get(p, 999))
            for p in partition_order:
                cs = cstats.get(p, {})
                total, used = cs.get('Total', 0), cs.get('Used', 0)
                part_usage[p] = used / total if total else 0.0
        except Exception:
            partition_order.sort(key=lambda p: -partition_gpus.get(p, 0))

        # Prune collapsed set to only current partitions
        self._ctx.collapsed &= set(partition_order)

        tree: List[TreeNode] = []
        for part in partition_order:
            count_cell = Text(str(partition_count.get(part, 0)), style='bold')
            gpu_total = partition_gpus.get(part, 0)
            gpu_cell = Text(str(gpu_total) if gpu_total else '', style='bold')

            # Partition divider as FoldGroup with COLLAPSED_SET
            children: List[TreeNode] = []
            for entry in partition_rows[part]:
                *cells, row_key, _part, jobs = entry
                rk = str(row_key)
                count = len(jobs)
                if count > 1:
                    # Job group as FoldGroup with EXPANDED_SET
                    ind_children = [
                        Leaf(key=f'ind::{job["jid"]}', data=job, indent=7)
                        for job in jobs
                    ]
                    children.append(FoldGroup(
                        key=rk,
                        fold_key=rk,
                        data={'cells': cells, 'jobs': jobs, 'row_key': row_key},
                        children=ind_children,
                        mode=FoldMode.EXPANDED_SET,
                        indent=3,
                    ))
                else:
                    children.append(Leaf(
                        key=rk,
                        data={'cells': cells, 'jobs': jobs, 'row_key': row_key},
                        indent=3,
                    ))

            tree.append(FoldGroup(
                key=f'div::{part}',
                fold_key=part,
                data={'label': part.upper(), 'count_cell': count_cell, 'gpu_cell': gpu_cell,
                      'usage_pct': part_usage.get(part, 0.0)},
                children=children,
                mode=FoldMode.COLLAPSED_SET,
                indent=0,
            ))
        return tree

    def _render_cell(self, node: TreeNode, is_collapsed: bool, n_cols: int) -> List[Text]:
        """Render a tree node into a list of Text cells."""
        if isinstance(node, FoldGroup) and node.mode == FoldMode.COLLAPSED_SET:
            # Partition divider with usage-proportional background
            d = node.data
            pct = d.get('usage_pct', 0.0)
            # Color matches cluster bar: green <50%, yellow 50-99%, red 100%
            tv = self.app.theme_variables
            if pct >= 1.0:
                bar_color = tc(tv, 'error', 'red')
            elif pct >= 0.50:
                bar_color = tc(tv, 'warning', 'yellow')
            else:
                bar_color = tc(tv, 'success', 'green')

            w = self._ctx.name_col_width
            label = d['label']
            # Build name cell with proportional colored fill
            filled = max(1, int(round(pct * w))) if pct > 0 else 0
            empty_w = w - filled
            name_cell = Text()
            name_cell.append(f'│ {label}'[:filled].ljust(filled), style=f'bold white on {bar_color}')
            if empty_w > 0:
                # Remaining label text without background
                remaining = f'│ {label}'[filled:]
                name_cell.append(remaining.ljust(empty_w)[:empty_w], style='bold black on white')

            cells = [Text(' ') for _ in range(self._ctx.n_cols)]
            cells[self._ctx.name_col_idx] = name_cell
            cells[1] = d['count_cell']
            cells[3] = d['gpu_cell']
            return cells

        if isinstance(node, FoldGroup) and node.mode == FoldMode.EXPANDED_SET:
            # Job group header
            d = node.data
            cells = list(d['cells'])
            jobs = d['jobs']
            orig_name = jobs[0]['name'] if jobs else cells[0].plain.strip()
            count = len(jobs)
            if count > 1:
                icon = '▼' if not is_collapsed else '▶'
                cells[0] = Text(f'   {icon} {orig_name}')
            else:
                cells[0] = Text(f'     {orig_name}')
            rk = str(d['row_key'])
            return [
                _color_flash(cells[i], self._ping.cell_intensity(rk, i))
                for i in range(len(cells))
            ]

        if isinstance(node, Leaf):
            if node.indent == 7:
                # Individual job under an expanded group
                job = node.data
                tv = self.app.theme_variables
                c_m = tc(tv, 'text-muted', 'bright_black')
                gpu_s = str(job['gpus']) if job['gpus'] else '-'
                job_start = job.get('start', '')
                job_ago = fmt_time(job_start) if job_start and job_start not in ('N/A', 'Unknown') else '—'
                return [
                    Text(f'   {"└" if node.is_last else "├"}── {job["name"]}', style=c_m),
                    Text(''),
                    Text(job['user'], style=c_m),
                    Text(gpu_s, style=c_m),
                    _state_text(job['state'], tv),
                    Text(job['elapsed'], style=c_m),
                    Text(job_ago, style=c_m),
                    Text(''),
                    Text(job['jid'], style=c_m),
                ]
            else:
                # Top-level single job
                d = node.data
                cells = list(d['cells'])
                jobs = d['jobs']
                orig_name = jobs[0]['name'] if jobs else cells[0].plain.strip()
                cells[0] = Text(f'     {orig_name}')
                rk = str(d['row_key'])
                return [
                    _color_flash(cells[i], self._ping.cell_intensity(rk, i))
                    for i in range(len(cells))
                ]

        return [Text('') for _ in range(n_cols)]

    def _rebuild_table(self, stats_text: Optional[Text], has_rows: bool) -> None:
        """Rebuild the DataTable from self._last_built using tree engine."""
        built = self._last_built
        dt    = self.query_one(SpeekDataTable)
        empty = self.query_one('#queue-empty', Static)
        n_cols = len(self._col_widths)

        self._tree = self._build_tree(built)
        self._rebuild(dt, self._ctx, self._tree)

        # Ghost rows for recently removed entries
        for ghost_key, g_intensity in self._ping.ghosts():
            bright = int(60 + g_intensity * 80)
            gs = f'dim #{bright:02x}{bright:02x}{bright:02x}'
            ghost = [Text('—', style=gs)] + [Text('', style=gs) for _ in range(n_cols - 1)]
            dt.add_row(*ghost, key=f'_ghost_{ghost_key}')

        # Schedule re-render while pings are fading
        if self._ping.has_active and not hasattr(self, '_ping_timer'):
            self._ping_timer = self.set_interval(2, self._tick_ping)
        elif not self._ping.has_active and hasattr(self, '_ping_timer'):
            self._ping_timer.stop()
            del self._ping_timer

        self.query_one(LoadingIndicator).display = False
        if has_rows:
            empty.display = False
        else:
            empty.update('No jobs currently in queue')
            empty.display = True

        stats_bar = self.query_one('#queue-stats', Static)
        if stats_text is not None:
            stats_bar.update(stats_text)
            stats_bar.display = True
        else:
            stats_bar.display = False

    def _tick_ping(self) -> None:
        """Re-render table while ping highlights are fading."""
        if self._ping.has_active:
            # Force a refresh by invalidating the sig
            self._last_queue_sig = frozenset()
            self._load()
        elif hasattr(self, '_ping_timer'):
            self._ping_timer.stop()
            del self._ping_timer

    def _selected_job_id(self):
        dt = self.query_one(SpeekDataTable)
        key = self._selected_key(dt, self._ctx)
        if not key or key.startswith(('div::', '_ghost_', 'ind::')):
            return None
        return key

    def action_toggle_fold(self) -> None:
        """Toggle fold on divider (partition) or job group row."""
        dt = self.query_one(SpeekDataTable)
        key = self._selected_key(dt, self._ctx)
        if not key or key.startswith(('_ghost_', 'ind::')):
            return
        self._toggle_and_rebuild(dt, self._ctx, self._tree, key)

    def action_fold_all(self) -> None:
        """Fold or unfold ALL partitions and collapse all expanded groups."""
        dt = self.query_one(SpeekDataTable)
        self._fold_all_and_rebuild(dt, self._ctx, self._tree)

    def action_job_detail(self) -> None:
        jid = self._selected_job_id()
        if not jid:
            return

        def _fetch() -> None:
            details = fetch_job_details(jid)
            self.app.call_from_thread(self._show_detail, jid, details)

        self.run_worker(_fetch, thread=True, group='job-detail')

    def _show_detail(self, jid: str, details: dict) -> None:
        from speek.speek_max.widgets.job_detail import JobDetailModal
        self.app.push_screen(JobDetailModal(jid, details))

    def action_refresh(self) -> None:
        self._load()
