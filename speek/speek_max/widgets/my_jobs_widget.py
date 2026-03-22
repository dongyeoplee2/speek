"""my_jobs_widget.py — Current user's jobs panel with project tree grouping."""
from __future__ import annotations

import re
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from rich.text import Text
from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widget import Widget

from speek.speek_max.widgets.modal_base import SpeekModal
from textual.widgets import Button, DataTable, Input, Label, LoadingIndicator, Static, TabbedContent, TabPane

from speek.speek_max.slurm import fetch_all_priorities, fetch_history, fetch_my_jobs, get_job_log_path
from speek.speek_max._utils import fmt_time, tc, safe, state_sym, state_badge
from speek.speek_max.widgets.datatable import SpeekDataTable
from speek.speek_max.widgets.foldable_table import (
    FoldableTableMixin, FoldGroup, FoldMode, Leaf, Divider, Spacer,
    TreeNode, TableContext, _build_divider_cells,
)


# ── Time dividers for history tab ─────────────────────────────────────────────

_HIST_DIV_PREFIX = '_hdiv_'
_MYJOBS_TC = '#myjobs-tc'
_MYJOBS_DT = '#myjobs-dt'
_MYHIST_DT = '#myjobs-hist-dt'

_HIST_TIME_ZONES = [
    (3600,         '< 1h ago'),
    (2 * 3600,     '1–2h ago'),
    (6 * 3600,     '2–6h ago'),
    (12 * 3600,    '6–12h ago'),
    (86400,        '12–24h ago'),
    (3 * 86400,    '1–3 days ago'),
    (7 * 86400,    '3–7 days ago'),
    (float('inf'), '> 1 week ago'),
]


def _hist_zone_idx(start_str: str) -> int:
    from datetime import datetime
    try:
        dt = datetime.strptime(start_str.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S')
        age = (datetime.now() - dt).total_seconds()
        for i, (bound, _) in enumerate(_HIST_TIME_ZONES):
            if age < bound:
                return i
    except Exception:
        pass
    return len(_HIST_TIME_ZONES) - 1


# ── Aggregation helpers ────────────────────────────────────────────────────────

_NAME_STRIP_RE = re.compile(r'[_\-]?\d+$')
_PROJ = 'proj::'
_IND  = 'ind::'


def _name_base(name: str) -> str:
    return _NAME_STRIP_RE.sub('', name).rstrip('_-').lower()


def _project_name(name: str) -> str:
    return _name_base(name) or name


def _should_merge(a: dict, b: dict) -> bool:
    if a['part'] != b['part'] or a['gpu'] != b['gpu'] or a['state'] != b['state']:
        return False
    na, nb = _name_base(a['name']), _name_base(b['name'])
    return bool(na and na == nb)


def _aggregate_within(items: List[dict]) -> List[dict]:
    """Merge similar jobs within a project into groups."""
    groups: List[dict] = []
    for item in items:
        for g in groups:
            if _should_merge(g, item):
                g['ids'].append(item['jid'])
                break
        else:
            groups.append({**item, 'ids': [item['jid']]})
    return groups


def _aggregate_by_project(rows: List[Tuple]) -> Dict[str, List[dict]]:
    """Group rows by project name (name_base), preserving order."""
    projects: Dict[str, List[dict]] = OrderedDict()
    for jid, name, part, gpus, state, elapsed, eta, start in rows:
        proj = _project_name(name)
        item = {'jid': jid, 'name': name, 'part': part, 'gpu': gpus,
                'state': state, 'elapsed': elapsed, 'eta': eta, 'start': start}
        projects.setdefault(proj, []).append(item)
    return projects


class CancelSelectModal(SpeekModal):
    """Step 1 of cancel flow: show job IDs for user to review/edit, then confirm."""

    def __init__(self, job_ids: List[str]) -> None:
        super().__init__()
        self._ids_str = ', '.join(job_ids)

    def compose(self) -> ComposeResult:
        """Compose the cancel select dialog."""
        with Vertical(id='cs-body', classes='speek-popup') as v:
            v.border_title = 'Cancel Jobs'
            yield Label('Job IDs to cancel (edit to remove any):', id='cs-hint')
            yield Input(value=self._ids_str, id='cs-input')
            with Horizontal(id='cs-btns'):
                yield Button('Cancel Jobs  [y]', id='cs-confirm')
                yield Button('Abort  [n]', id='cs-abort')

    def on_mount(self) -> None:
        self._bindings.bind('y', 'confirm')
        self._bindings.bind('n', 'dismiss_none')
        self._bindings.bind('escape', 'dismiss_none')
        self.query_one('#cs-input', Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'cs-confirm':
            self.action_confirm()
        elif event.button.id == 'cs-abort':
            self.action_dismiss_none()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.action_confirm()

    def action_confirm(self) -> None:
        val = self.query_one('#cs-input', Input).value.strip()
        self.dismiss(val if val else None)

    def action_dismiss_none(self) -> None:
        self.dismiss(None)


class MyJobsWidget(FoldableTableMixin, Widget):
    """Current user's running + pending jobs grouped by project."""

    BORDER_TITLE = "My Jobs"
    can_focus = True

    BINDINGS = [
        Binding('d',     'view_job',    'Details',  show=True),
        Binding('enter', 'view_job',   'Details',  show=False),
        Binding('v',     'toggle_fold', '▶▼',       show=True),
        Binding('V',     'fold_all',   '',         show=False),
        Binding('x',     'cancel_job', 'Cancel',   show=True),
        Binding('1',     'tab_current', '',        show=False),
        Binding('2',     'tab_history', '',        show=False),
        Binding('r',     'refresh',    'Refresh',  show=True),
        Binding('l',     'view_job',   'Details',  show=False),
        Binding('e',     'view_job',   'Details',  show=False),
    ]

    # ── Messages ──────────────────────────────────────────────────────────────

    class CancelRequested(Message):
        def __init__(self, job_ids: List[str]) -> None:
            super().__init__()
            self.job_ids = job_ids

    class RunningCount(Message):
        def __init__(self, running: int, pending: int) -> None:
            super().__init__()
            self.running = running
            self.pending = pending

    class OomCount(Message):
        def __init__(self, count: int) -> None:
            super().__init__()
            self.count = count

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def __init__(self, user: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.user = user

    def compose(self) -> ComposeResult:
        """Compose the my-jobs widget."""
        yield LoadingIndicator()
        with TabbedContent(id='myjobs-tc', initial='tc-current'):
            with TabPane('1 Current', id='tc-current'):
                yield Static('', id='myjobs-empty', classes='empty-state')
                yield SpeekDataTable(id='myjobs-dt', cursor_type='row', show_cursor=True)
                yield Static('', id='myjobs-stats')
            with TabPane('2 History', id='tc-history'):
                yield SpeekDataTable(id='myjobs-hist-dt', cursor_type='row', show_cursor=True)
                yield Static('', id='myjobs-hist-empty', classes='empty-state')

    def on_mount(self) -> None:
        self._log_hints: Dict[str, str] = {}  # first_jid → last log line
        self._oom_jobs: set[str] = set()       # jids with OOM detected
        self._oom_notified: set[str] = set()  # jids already notified
        self._group_ids: Dict[str, List[str]] = {}  # first_jid → all jids in group
        self._job_data: Dict[str, dict] = {}  # jid → individual item dict
        self._last_rows: List[Tuple] = []
        self._last_priorities: Dict = {}
        self._last_myjobs_sig: frozenset = frozenset()

        # Current tab context + tree
        dt = self.query_one(SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('Name',      width=22)
        dt.add_column('#J',         width=2)
        dt.add_column('#G',       width=3)
        dt.add_column('Part',      width=5)
        dt.add_column('State',     width=3)
        dt.add_column('Elapsed',   width=7)
        dt.add_column('Ago',       width=5)
        dt.add_column('ETA',       width=5)
        dt.add_column('Rank',      width=4)
        dt.add_column('IDs',       width=14)
        self._log_col = dt.add_column('Log', width=16)
        self._current_ctx = self._init_ctx(renderer=self._render_current_cell, n_cols=11, name_col_width=22)
        self._current_tree: List[TreeNode] = []

        # History tab context + tree
        hdt = self.query_one(_MYHIST_DT, SpeekDataTable)
        hdt.zebra_stripes = True
        hdt.add_column('Name',      width=22)
        hdt.add_column('#J',         width=2)
        hdt.add_column('#G',       width=3)
        hdt.add_column('Part',      width=5)
        hdt.add_column('State',     width=3)
        hdt.add_column('Elapsed',   width=7)
        hdt.add_column('Ago',       width=5)
        hdt.add_column('IDs',       width=14)
        self._history_ctx = self._init_ctx(renderer=self._render_history_cell, n_cols=8, name_col_width=22)
        self._history_tree: List[TreeNode] = []
        self._hist_first_load: bool = True
        self._last_hist_sig: str = ''

        self._load()
        self.set_timer(3.0, self._load_history)
        interval = getattr(self.app, '_queue_refresh', 5)
        self._refresh_timer = self.set_interval(interval, self._load)
        self.set_interval(60, self._load_history)

    def on_click(self, event) -> None:
        try:
            self._active_dt().focus()
        except Exception:
            pass

    def set_refresh_interval(self, seconds: int) -> None:
        try:
            self._refresh_timer.stop()
        except Exception:
            pass
        self._refresh_timer = self.set_interval(seconds, self._load)

    # ── Data ──────────────────────────────────────────────────────────────────

    def _load(self) -> None:
        if not getattr(self.app, '_cmd_squeue', True):
            try:
                self.query_one(LoadingIndicator).display = False
                e = self.query_one('#myjobs-empty', Static)
                e.update('[dim]squeue unavailable on this cluster[/dim]')
                e.display = True
            except Exception:
                pass
            return
        tv = self.app.theme_variables  # snapshot on main thread
        self.run_worker(
            lambda: self._fetch(tv), thread=True, exclusive=True, group='myjobs',
        )

    def _fetch(self, tv: dict) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        rows       = fetch_my_jobs(self.user)
        priorities = fetch_all_priorities()
        if worker.is_cancelled:
            return
        args = self._pre_build_myjobs(rows, priorities, tv)
        self.app.call_from_thread(self._update, *args)

    @staticmethod
    def _pre_build_myjobs(
        rows: List[Tuple], priorities: Optional[Dict], tv: dict,
    ) -> tuple:
        """Pure computation — safe to run in a worker thread."""
        sig         = frozenset((r[0], r[1], r[2], r[3], r[4]) for r in rows)
        projects    = _aggregate_by_project(rows)
        part_ranked, part_rank_index = MyJobsWidget._build_rank_index(priorities or {})
        c_muted     = tc(tv, 'text-muted',     'bright_black')
        c_secondary = tc(tv, 'text-secondary', 'default')
        c_success   = tc(tv, 'text-success',   'green')
        c_warning   = tc(tv, 'text-warning',   'yellow')
        c_error     = tc(tv, 'text-error',     'red')
        colors      = (c_muted, c_secondary, c_success, c_warning, c_error)
        all_items   = [it for items in projects.values() for it in items]
        stats_text  = None
        n_run = n_pend = 0
        if all_items:
            n_run  = sum(1 for it in all_items if it['state'] == 'RUNNING')
            n_pend = sum(1 for it in all_items if it['state'] == 'PENDING')
            n_gpu  = sum(int(it['gpu']) for it in all_items if it['gpu'].isdigit())
            t = Text()
            t.append(str(n_run),  style=f'bold {c_success}')
            t.append(' running  ', style=c_muted)
            t.append(str(n_pend), style=f'bold {c_warning}')
            t.append(' pending  ', style=c_muted)
            t.append(str(n_gpu),  style=f'bold {c_secondary}')
            t.append(' GPUs', style=c_muted)
            stats_text = t
        return (sig, rows, priorities, projects,
                part_ranked, part_rank_index, stats_text, n_run, n_pend, colors)

    @safe('MyJobs rebuild')
    def _rebuild_from_last(self) -> None:
        """Force a re-render from cached rows (called by collapse/expand actions)."""
        self._last_myjobs_sig = frozenset()  # invalidate so _update always rebuilds
        args = self._pre_build_myjobs(
            self._last_rows, self._last_priorities, self.app.theme_variables,
        )
        self._update(*args)

    @staticmethod
    def _build_rank_index(priorities: Dict) -> Tuple[Dict, Dict]:
        part_ranked: Dict[str, list] = {}
        for jid_p, info in (priorities or {}).items():
            p = info.get('partition', '')
            part_ranked.setdefault(p, []).append((jid_p, info.get('total', 0)))
        for p in part_ranked:
            part_ranked[p].sort(key=lambda x: -x[1])
        part_rank_index: Dict[str, Dict[str, int]] = {
            p: {jid_p: i + 1 for i, (jid_p, _) in enumerate(lst)}
            for p, lst in part_ranked.items()
        }
        return part_ranked, part_rank_index

    def _rank_cell(self, g: dict, priorities: Dict,
                   part_ranked: Dict, part_rank_index: Dict,
                   c_warning: str, c_muted: str) -> Text:
        if g['state'] != 'PENDING' or not priorities:
            return Text('—', style=c_muted)
        rank_pos = part_rank_index.get(g['part'], {}).get(g['ids'][0])
        total = len(part_ranked.get(g['part'], []))
        if rank_pos is not None:
            return Text(f'#{rank_pos}/{total}', style=c_warning)
        return Text('—', style=c_muted)

    @staticmethod
    def _parse_elapsed(s: str) -> int:
        """Parse elapsed string like '1-02:03:04' or '12:34' to total seconds."""
        try:
            days = 0
            if '-' in s:
                d, s = s.split('-', 1)
                days = int(d)
            parts = s.split(':')
            if len(parts) == 3:
                return days * 86400 + int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            if len(parts) == 2:
                return days * 86400 + int(parts[0]) * 60 + int(parts[1])
            return 0
        except Exception:
            return 0

    @staticmethod
    def _fmt_elapsed(secs: int) -> str:
        """Format seconds to compact elapsed string."""
        if secs <= 0:
            return ''
        d, r = divmod(secs, 86400)
        h, r = divmod(r, 3600)
        m, _ = divmod(r, 60)
        if d:
            return f'{d}d{h:02d}h'
        if h:
            return f'{h}:{m:02d}'
        return f'{m}m'

    # ── Current tab: tree building ────────────────────────────────────────────

    def _build_current_tree(
        self, projects: Dict[str, List[dict]], priorities: Dict,
        part_ranked: Dict, part_rank_index: Dict, colors: tuple,
    ) -> List[TreeNode]:
        """Build tree for the Current tab."""
        c_muted, c_secondary, c_success, c_warning, c_error = colors
        state_style = {
            'RUNNING': f'bold {c_success}',
            'PENDING': f'bold {c_warning}',
            'FAILED':  f'bold {c_error}',
            'TIMEOUT': f'bold {c_error}',
            'CANCELLED': f'bold {c_muted}',
            'COMPLETED': 'bold #4A9FD9',
            'OUT_OF_MEMORY': f'bold {c_error}',
        }
        tree: List[TreeNode] = []
        new_first_jids: List[str] = []

        for proj, items in projects.items():
            # Store job data for individual rows
            for item in items:
                self._job_data[item['jid']] = item

            agg_groups = _aggregate_within(items)
            children: List[TreeNode] = []
            for g in agg_groups:
                first_jid = g['ids'][0]
                self._group_ids[first_jid] = g['ids']
                new_first_jids.append(first_jid)
                count = len(g['ids'])
                if count > 1:
                    ind_children = [
                        Leaf(key=f'ind::{jid}', data={'jid': jid, 'group': g, 'state_style': state_style, 'colors': colors}, indent=7)
                        for jid in g['ids']
                    ]
                    children.append(FoldGroup(
                        key=first_jid,
                        fold_key=first_jid,
                        data={
                            'group': g, 'priorities': priorities,
                            'part_ranked': part_ranked, 'part_rank_index': part_rank_index,
                            'state_style': state_style, 'colors': colors,
                        },
                        children=ind_children,
                        mode=FoldMode.EXPANDED_SET,
                        indent=3,
                    ))
                else:
                    children.append(Leaf(
                        key=first_jid,
                        data={
                            'group': g, 'priorities': priorities,
                            'part_ranked': part_ranked, 'part_rank_index': part_rank_index,
                            'state_style': state_style, 'colors': colors,
                        },
                        indent=3,
                    ))

            tree.append(FoldGroup(
                key=f'{_PROJ}{proj}',
                fold_key=proj,
                data={'proj': proj, 'items': items, 'colors': colors},
                children=children,
                mode=FoldMode.COLLAPSED_SET,
                indent=0,
            ))

        self._new_first_jids = new_first_jids
        return tree

    def _render_current_cell(self, node: TreeNode, is_collapsed: bool, n_cols: int) -> List[Text]:
        """Render a Current tab tree node."""
        if isinstance(node, FoldGroup) and node.mode == FoldMode.COLLAPSED_SET:
            # Project header
            return self._render_project_header(node, is_collapsed)

        if isinstance(node, FoldGroup) and node.mode == FoldMode.EXPANDED_SET:
            # Job group header (with fold icon)
            return self._render_job_group(node, is_collapsed, with_fold=True)

        if isinstance(node, Leaf):
            if node.indent == 7:
                return self._render_individual_job(node)
            elif node.indent == 3:
                return self._render_job_group_leaf(node)

        return [Text('') for _ in range(n_cols)]

    def _render_project_header(self, node: FoldGroup, is_collapsed: bool) -> List[Text]:
        d = node.data
        proj = d['proj']
        items = d['items']
        c_muted, c_secondary, c_success, c_warning, c_error = d['colors']

        total = sum(len(it.get('ids', [it])) for it in items)
        n_run  = sum(1 for it in items if it['state'] == 'RUNNING')
        n_pend = sum(1 for it in items if it['state'] == 'PENDING')
        n_gpu  = sum(int(it['gpu']) * len(it.get('ids', [it])) for it in items if it.get('gpu', '').isdigit())
        max_secs = max((self._parse_elapsed(it.get('elapsed', '')) for it in items), default=0)
        elapsed_str = self._fmt_elapsed(max_secs)
        state_t = Text()
        if n_run:
            state_t.append(str(n_run), style=f'bold {c_success}')
            state_t.append('R ', style=c_muted)
        if n_pend:
            state_t.append(str(n_pend), style=f'bold {c_warning}')
            state_t.append('P', style=c_muted)
        # Earliest start across all items in the project
        earliest_start = ''
        for it in items:
            s = it.get('start', '')
            if s and s not in ('N/A', 'Unknown') and (not earliest_start or s < earliest_start):
                earliest_start = s
        ago_str = fmt_time(earliest_start) if earliest_start else '—'

        icon = '▶' if is_collapsed else '▼'
        return [
            Text(f'{icon} {proj}', style='bold'),
            Text(str(total), style=f'bold {c_secondary}'),
            Text(str(n_gpu) if n_gpu else '', style=f'bold {c_secondary}'),
            Text(' '),
            state_t,
            Text(elapsed_str, style=c_muted),
            Text(ago_str, style=c_muted),
            Text(' '),
            Text(' '),
            Text(' '),
            Text('', style=c_muted),
        ]

    def _render_job_group(self, node: FoldGroup, is_collapsed: bool, with_fold: bool = True) -> List[Text]:
        d = node.data
        g = d['group']
        colors = d['colors']
        c_muted, c_secondary, c_success, c_warning, c_error = colors
        state_style = d['state_style']
        priorities = d['priorities']
        part_ranked = d['part_ranked']
        part_rank_index = d['part_rank_index']

        count = len(g['ids'])
        first_jid = g['ids'][0]
        fold_icon = '▼ ' if not is_collapsed else '▶ '
        ids_str = first_jid if count == 1 else f'{first_jid}+{count - 1}'
        rank = self._rank_cell(g, priorities, part_ranked, part_rank_index, c_warning, c_muted)
        hint = self._log_hints.get(first_jid, '')
        has_oom = any(jid in self._oom_jobs for jid in g['ids'])
        if has_oom:
            state_text = state_badge('OUT_OF_MEMORY')
        else:
            state_text = state_badge(g['state'])
        hint_style = f'bold {c_error}' if has_oom else c_muted
        try:
            total_gpu = int(g['gpu']) * count
        except (ValueError, TypeError):
            total_gpu = 0
        g_start = g.get('start', '')
        g_ago = fmt_time(g_start) if g_start and g_start not in ('N/A', 'Unknown') else '—'
        return [
            Text(f'  {fold_icon}{g["name"]}', style='default'),
            Text(str(count), style=f'bold {c_muted}'),
            Text(str(total_gpu) if total_gpu else g['gpu'], style='bold'),
            Text(g['part'], style=c_secondary),
            state_text,
            Text(g['elapsed'], style=c_muted),
            Text(g_ago, style=c_muted),
            Text(g['eta'], style=f'{c_warning} italic') if g['eta'] else Text(''),
            rank,
            Text(ids_str, style=c_muted),
            Text(hint[:32], style=hint_style),
        ]

    def _render_job_group_leaf(self, node: Leaf) -> List[Text]:
        """Render a single-job group (no fold icon, just indented)."""
        d = node.data
        g = d['group']
        colors = d['colors']
        c_muted, c_secondary, c_success, c_warning, c_error = colors
        state_style = d['state_style']
        priorities = d['priorities']
        part_ranked = d['part_ranked']
        part_rank_index = d['part_rank_index']

        first_jid = g['ids'][0]
        rank = self._rank_cell(g, priorities, part_ranked, part_rank_index, c_warning, c_muted)
        hint = self._log_hints.get(first_jid, '')
        has_oom = any(jid in self._oom_jobs for jid in g['ids'])
        if has_oom:
            state_text = state_badge('OUT_OF_MEMORY')
        else:
            state_text = state_badge(g['state'])
        hint_style = f'bold {c_error}' if has_oom else c_muted
        g_start = g.get('start', '')
        g_ago = fmt_time(g_start) if g_start and g_start not in ('N/A', 'Unknown') else '—'
        return [
            Text(f'    {g["name"]}', style='default'),
            Text('1', style=f'bold {c_muted}'),
            Text(g['gpu'], style='bold'),
            Text(g['part'], style=c_secondary),
            state_text,
            Text(g['elapsed'], style=c_muted),
            Text(g_ago, style=c_muted),
            Text(g['eta'], style=f'{c_warning} italic') if g['eta'] else Text(''),
            rank,
            Text(first_jid, style=c_muted),
            Text(hint[:32], style=hint_style),
        ]

    def _render_individual_job(self, node: Leaf) -> List[Text]:
        d = node.data
        jid = d['jid']
        g = d['group']
        state_style = d['state_style']
        colors = d['colors']
        c_muted, c_secondary, _, c_warning, _ = colors

        it = self._job_data.get(jid, g)
        state = it.get('state', g['state'])
        eta = it.get('eta', '')
        it_start = it.get('start', '')
        it_ago = fmt_time(it_start) if it_start and it_start not in ('N/A', 'Unknown') else '—'
        return [
            Text(f'  {"└" if node.is_last else "├"}── {jid}', style=c_muted),
            Text('', style=c_muted),
            Text(it.get('gpu', g['gpu']), style='bold'),
            Text(it.get('part', g['part']), style=c_secondary),
            state_badge(state),
            Text(it.get('elapsed', g['elapsed']), style=c_muted),
            Text(it_ago, style=c_muted),
            Text(eta, style=f'{c_warning} italic') if eta else Text(''),
            Text('', style=c_muted),
            Text(jid, style=c_muted),
            Text('', style=c_muted),
        ]

    # ── History tab: tree building ────────────────────────────────────────────

    def _build_history_tree(self, rows: List[Tuple]) -> List[TreeNode]:
        """Build tree for the History tab."""
        import re as _re

        # Group by project
        projects: OrderedDict[str, list] = OrderedDict()
        for r in rows:
            jid, name, part = r[0], r[1], r[2]
            start, elapsed, state = r[3], r[4], r[5]
            gpu_str = r[7] if len(r) > 7 else ''
            gpu_model = ''
            gpu_count = '0'
            m = _re.search(r'gres/gpu(?::([^:,]+))?(?::(\d+)|=(\d+))', gpu_str)
            if m:
                gpu_model = m.group(1) or ''
                gpu_count = m.group(2) or m.group(3) or '1'
            proj = _name_base(name) or name
            projects.setdefault(proj, []).append({
                'jid': jid, 'name': name, 'part': gpu_model or part,
                'state': state, 'elapsed': elapsed, 'start': start,
                'gpu': gpu_count,
            })

        # Sort projects by most recent activity (latest start or end time)
        def _latest_activity(items):
            from datetime import datetime as _dt, timedelta as _td
            latest = ''
            for it in items:
                s = it.get('start', '')
                if s and s not in ('N/A', 'Unknown', 'None'):
                    # Compute end = start + elapsed
                    try:
                        st = _dt.strptime(s.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S')
                        e_secs = self._parse_elapsed(it.get('elapsed', ''))
                        end = (st + _td(seconds=e_secs)).strftime('%Y-%m-%d %H:%M:%S')
                        candidate = max(s, end)
                    except Exception:
                        candidate = s
                    if candidate > latest:
                        latest = candidate
            return latest

        sorted_projects = sorted(projects.items(),
                                  key=lambda kv: _latest_activity(kv[1]),
                                  reverse=True)

        # On first load, start with all projects collapsed
        if self._hist_first_load:
            self._hist_first_load = False
            for proj, _ in sorted_projects:
                self._history_ctx.collapsed.add(proj)

        tree: List[TreeNode] = []
        current_zone = -1
        div_counter = 0

        for proj, items in sorted_projects:
            # Time divider based on most recent activity in project
            zone = _hist_zone_idx(_latest_activity(items))
            if zone != current_zone:
                if current_zone >= 0:
                    tree.append(Spacer(key=f'{_HIST_DIV_PREFIX}{div_counter}_sp'))
                current_zone = zone
                from datetime import datetime as _dt, timedelta as _td
                _now = _dt.now()
                _bounds = [b for b, _ in _HIST_TIME_ZONES]
                _labels = ['1h', '2h', '6h', '12h', '1d', '3d', '7d', '7d+']
                age = _labels[min(zone, len(_labels)-1)]
                if zone >= 5:
                    secs = _bounds[zone]
                    date_dt = _now - _td(seconds=secs) if secs != float('inf') else _now - _td(seconds=_bounds[zone-1])
                    date_str = date_dt.strftime('%y-%m-%d')
                elif zone >= 4:
                    date_str = (_now - _td(seconds=_bounds[zone])).strftime('%y-%m-%d')
                else:
                    date_str = _now.strftime('%H:%M')
                tree.append(Divider(
                    key=f'{_HIST_DIV_PREFIX}{div_counter}',
                    label=f'{age} {date_str}',
                ))
                div_counter += 1

            n = len(items)
            if n > 1:
                # Project FoldGroup (COLLAPSED_SET)
                # Sub-group similar jobs within the project
                sub_groups: list[list] = []
                for item in items:
                    merged = False
                    for sg in sub_groups:
                        if sg[0]['name'] == item['name'] or (
                            _name_base(sg[0]['name']) and _name_base(sg[0]['name']) == _name_base(item['name'])
                        ):
                            sg.append(item)
                            merged = True
                            break
                    if not merged:
                        sub_groups.append([item])

                proj_children: List[TreeNode] = []
                for sg in sub_groups:
                    sg_n = len(sg)
                    first = sg[0]
                    if sg_n > 1:
                        sg_key = f'hgrp_{first["jid"]}'
                        ind_children = [
                            Leaf(key=f'hist_{item["jid"]}', data=item, indent=7)
                            for item in sg
                        ]
                        proj_children.append(FoldGroup(
                            key=sg_key,
                            fold_key=sg_key,
                            data={'items': sg, 'first': first},
                            children=ind_children,
                            mode=FoldMode.COLLAPSED_SET,
                            indent=5,
                        ))
                    else:
                        proj_children.append(Leaf(
                            key=f'hist_{first["jid"]}',
                            data=first,
                            indent=5,
                        ))

                tree.append(FoldGroup(
                    key=f'hproj::{proj}',
                    fold_key=proj,
                    data={'proj': proj, 'items': items},
                    children=proj_children,
                    mode=FoldMode.COLLAPSED_SET,
                    indent=3,
                ))
            else:
                # Single-item project — just a leaf under the divider
                first = items[0]
                tree.append(Leaf(
                    key=f'hist_{first["jid"]}',
                    data=first,
                    indent=5,
                ))

        return tree

    def _render_history_cell(self, node: TreeNode, is_collapsed: bool, n_cols: int) -> List[Text]:
        """Render a History tab tree node."""
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        c_error = tc(tv, 'text-error', 'red')
        state_style = {
            'RUNNING': f'bold {c_success}',
            'PENDING': f'bold {c_warning}',
            'COMPLETED': 'bold #4A9FD9',
            'FAILED': f'bold {c_error}',
            'TIMEOUT': f'bold {c_error}',
            'CANCELLED': f'bold {c_muted}',
            'OUT_OF_MEMORY': f'bold {c_error}',
        }

        if isinstance(node, FoldGroup) and node.key.startswith('hproj::'):
            return self._render_hist_project(node, is_collapsed,
                                              c_muted, c_success, c_warning, c_error)

        if isinstance(node, FoldGroup) and node.key.startswith('hgrp_'):
            return self._render_hist_subgroup(node, is_collapsed,
                                               c_muted, c_error, state_style)

        if isinstance(node, Leaf):
            item = node.data
            ss = state_style.get(item['state'].split()[0], c_muted)
            h_start = item.get('start', '')
            h_ago = fmt_time(h_start) if h_start and h_start not in ('N/A', 'Unknown') else '—'
            if node.indent == 7:
                # Individual job under sub-group
                return [
                    Text(f'     {"└" if node.is_last else "├"}── {item["name"]}', style=c_muted),
                    Text('', style=c_muted),
                    Text(item['gpu'], style=c_muted),
                    Text(item['part'], style=c_muted),
                    state_badge(item['state']),
                    Text(item['elapsed'], style=c_muted),
                    Text(h_ago, style=c_muted),
                    Text(item['jid'], style=c_muted),
                ]
            else:
                # Single job (indent=5)
                return [
                    Text(f'     {item["name"]}'),
                    Text('', style=c_muted),
                    Text(item['gpu'], style=c_muted),
                    Text(item['part'], style=c_muted),
                    state_badge(item['state']),
                    Text(item['elapsed'], style=c_muted),
                    Text(h_ago, style=c_muted),
                    Text(item['jid'], style=c_muted),
                ]

        return [Text('') for _ in range(n_cols)]

    def _render_hist_project(
        self, node: FoldGroup, is_collapsed: bool,
        c_muted: str, c_success: str, c_warning: str, c_error: str,
    ) -> List[Text]:
        d = node.data
        items = d['items']
        proj = d['proj']
        n = len(items)
        n_ok = sum(1 for i in items if i['state'] == 'COMPLETED')
        n_fail = sum(1 for i in items if i['state'] in ('FAILED', 'TIMEOUT', 'OUT_OF_MEMORY'))
        n_run = sum(1 for i in items if i['state'] == 'RUNNING')
        n_pend = sum(1 for i in items if i['state'] == 'PENDING')
        state_t = Text()
        if n_run:
            state_t.append(f'{n_run}', style=f'bold {c_success}')
            state_t.append('R ', style=c_muted)
        if n_ok:
            state_t.append(f'{n_ok}', style=c_muted)
            state_t.append('C ', style=c_muted)
        if n_fail:
            state_t.append(f'{n_fail}', style=f'bold {c_error}')
            state_t.append('F ', style=c_muted)
        if n_pend:
            state_t.append(f'{n_pend}', style=c_warning)
            state_t.append('P', style=c_muted)

        from datetime import datetime as _dt2, timedelta as _td2
        earliest = None
        latest = None
        for it in items:
            try:
                s = _dt2.strptime(it['start'].replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S')
                e_secs = self._parse_elapsed(it['elapsed'])
                end = s + _td2(seconds=e_secs)
                if earliest is None or s < earliest:
                    earliest = s
                if latest is None or end > latest:
                    latest = end
            except Exception:
                pass
        proj_elapsed = ''
        if earliest and latest:
            proj_elapsed = self._fmt_elapsed(int((latest - earliest).total_seconds()))

        n_gpu = sum(int(it['gpu']) for it in items if it.get('gpu', '').isdigit())
        # Earliest start across all items
        earliest_start = ''
        for it in items:
            s = it.get('start', '')
            if s and s not in ('N/A', 'Unknown') and (not earliest_start or s < earliest_start):
                earliest_start = s
        proj_ago = fmt_time(earliest_start) if earliest_start else '—'

        icon = '▶' if is_collapsed else '▼'
        return [
            Text(f'   {icon} {proj}', style='bold'),
            Text(str(n), style=f'bold {c_muted}'),
            Text(str(n_gpu) if n_gpu else '', style=f'bold {c_muted}'),
            Text(items[0]['part'], style=c_muted),
            state_t,
            Text(proj_elapsed, style=c_muted),
            Text(proj_ago, style=c_muted),
            Text(' '),
        ]

    def _render_hist_subgroup(
        self, node: FoldGroup, is_collapsed: bool,
        c_muted: str, c_error: str, state_style: dict,
    ) -> List[Text]:
        d = node.data
        sg = d['items']
        first = d['first']
        sg_n = len(sg)
        sg_ok = sum(1 for i in sg if i['state'] == 'COMPLETED')
        sg_fail = sum(1 for i in sg if i['state'] in ('FAILED', 'TIMEOUT', 'OUT_OF_MEMORY'))
        sg_state = Text()
        if sg_ok:
            sg_state.append(f'{sg_ok}', style=c_muted)
            sg_state.append('C ', style=c_muted)
        if sg_fail:
            sg_state.append(f'{sg_fail}', style=f'bold {c_error}')
            sg_state.append('F', style=c_muted)
        icon = '▶' if is_collapsed else '▼'
        sg_gpu = sum(int(it['gpu']) for it in sg if it.get('gpu', '').isdigit())
        sg_start = first.get('start', '')
        sg_ago = fmt_time(sg_start) if sg_start and sg_start not in ('N/A', 'Unknown') else '—'
        return [
            Text(f'     {icon} {first["name"]}', style=''),
            Text(f' {sg_n}', style=c_muted),
            Text(str(sg_gpu) if sg_gpu else '', style=c_muted),
            Text(first['part'], style=c_muted),
            sg_state,
            Text('', style=c_muted),
            Text(sg_ago, style=c_muted),
            Text(f'{first["jid"]}+{sg_n-1}', style=c_muted),
        ]

    # ── Update methods ────────────────────────────────────────────────────────

    def _update(
        self, sig: frozenset, rows: List[Tuple], priorities: Optional[Dict],
        projects: Dict, part_ranked: Dict, part_rank_index: Dict,
        stats_text: Optional[Text], n_run: int, n_pend: int, colors: tuple,
    ) -> None:
        # Always hide loading indicator, stats bar, and show empty state if needed
        self.query_one(LoadingIndicator).display = False
        empty = self.query_one('#myjobs-empty', Static)
        stats_bar = self.query_one('#myjobs-stats', Static)
        if not rows:
            empty.update(f'No active or pending jobs for {self.user}')
            empty.display = True
            stats_bar.display = False
        else:
            empty.display = False

        if sig == self._last_myjobs_sig:
            self._last_rows = rows
            self._last_priorities = priorities or {}
            return
        self._last_myjobs_sig = sig
        self._last_rows = rows
        self._last_priorities = priorities or {}

        dt = self.query_one(SpeekDataTable)

        # Prune stale data
        self._current_ctx.collapsed &= set(projects.keys())
        live_jids = {it['jid'] for items in projects.values() for it in items}
        self._log_hints = {k: v for k, v in self._log_hints.items() if k in live_jids}
        self._job_data = {}
        self._group_ids = {}

        self._current_tree = self._build_current_tree(
            projects, priorities, part_ranked, part_rank_index, colors,
        )
        self._rebuild(dt, self._current_ctx, self._current_tree)

        self._current_ctx.expanded &= set(self._group_ids.keys())
        self.query_one(LoadingIndicator).display = False

        stats_bar = self.query_one('#myjobs-stats', Static)
        if stats_text is not None:
            stats_bar.update(stats_text)
            stats_bar.display = True
        else:
            stats_bar.display = False

        self.post_message(self.RunningCount(n_run, n_pend))

        uncached = [j for j in self._new_first_jids if j not in self._log_hints]
        if uncached:
            self.run_worker(
                lambda jids=uncached: self._fetch_log_hints(jids),
                thread=True, group='log-hints',
            )

    def _fetch_log_hints(self, jids: List[str]) -> Dict:
        from speek.speek_max.log_scan import extract_hint, detect_oom
        hints: Dict[str, str] = {}
        oom_jids: set[str] = set()
        for jid in jids:
            path = get_job_log_path(jid)
            if path:
                hints[jid] = extract_hint(path) or ''
                # Check for OOM in running jobs
                oom_msg = detect_oom(path)
                if oom_msg:
                    oom_jids.add(jid)
                    if not hints[jid].startswith('⚠'):
                        hints[jid] = f'⚠ OOM: {oom_msg[:28]}'
            else:
                hints[jid] = ''
        return {'hints': hints, 'oom': oom_jids}

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.state != WorkerState.SUCCESS:
            return
        if event.worker.group == 'log-hints':
            result = event.worker.result or {}
            if isinstance(result, dict) and 'hints' in result:
                self._log_hints.update(result['hints'])
                self._oom_jobs |= result.get('oom', set())
                self.post_message(self.OomCount(len(self._oom_jobs)))
            else:
                self._log_hints.update(result)
            self._apply_log_hints()

    def _apply_log_hints(self) -> None:
        dt = self.query_one(SpeekDataTable)
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_error = tc(tv, 'text-error', 'red')
        for jid, hint in self._log_hints.items():
            try:
                style = f'bold {c_error}' if jid in self._oom_jobs else c_muted
                dt.update_cell(jid, self._log_col, Text(hint[:32], style=style))
            except Exception:
                pass

    @on(DataTable.RowHighlighted)
    def _on_row_highlighted(self, _event: DataTable.RowHighlighted) -> None:
        pass

    # ── Selection helpers ──────────────────────────────────────────────────────

    def _active_tab(self) -> str:
        try:
            return self.query_one(_MYJOBS_TC, TabbedContent).active.removeprefix('tc-')
        except Exception:
            return 'current'

    def _active_dt(self) -> SpeekDataTable:
        if self._active_tab() == 'history':
            try:
                return self.query_one(_MYHIST_DT, SpeekDataTable)
            except Exception:
                pass
        return self.query_one(_MYJOBS_DT, SpeekDataTable)

    def _active_ctx(self) -> TableContext:
        if self._active_tab() == 'history':
            return self._history_ctx
        return self._current_ctx

    def _active_tree(self) -> List[TreeNode]:
        if self._active_tab() == 'history':
            return self._history_tree
        return self._current_tree

    def _active_row_keys(self) -> List[str]:
        return self._active_ctx().row_keys

    def _selected_key_val(self) -> Optional[str]:
        dt = self._active_dt()
        ctx = self._active_ctx()
        return self._selected_key(dt, ctx)

    def _selected_job_id(self) -> Optional[str]:
        key = self._selected_key_val()
        if not key:
            return None
        # History tab keys
        if key.startswith('hist_'):
            return key[5:]
        if key.startswith('hproj::') or key.startswith(_HIST_DIV_PREFIX) or key.startswith('hgrp_'):
            return None
        # Current tab keys
        if key.startswith(_IND):
            return key.removeprefix(_IND)
        if key.startswith(_PROJ):
            keys = self._active_row_keys()
            dt = self._active_dt()
            try:
                cursor_row = dt.cursor_row
            except Exception:
                return None
            for i in range(cursor_row + 1, len(keys)):
                v = keys[i]
                if v.startswith(_PROJ):
                    break
                if v.startswith(_IND):
                    return v.removeprefix(_IND)
                return v
            return None
        return key

    def _jids_in_project(self, proj_key: str) -> List[str]:
        """Return all job IDs under a project row key."""
        result: List[str] = []
        past_header = False
        keys = self._current_ctx.row_keys
        for k in keys:
            if k == proj_key:
                past_header = True
                continue
            if not past_header:
                continue
            if k.startswith(_PROJ):
                break
            if not k.startswith(_IND):
                result.extend(self._group_ids.get(k, [k]))
        return result

    def _all_jids_in_selection(self) -> List[str]:
        """Return all job IDs for the current cursor position."""
        key = self._selected_key_val()
        if not key:
            return []
        if key.startswith(_IND):
            return [key.removeprefix(_IND)]
        if key.startswith(_PROJ):
            return self._jids_in_project(key)
        return self._group_ids.get(key, [key])

    # ── Actions ───────────────────────────────────────────────────────────────

    def action_fold_all(self) -> None:
        """Fold or unfold ALL groups at once."""
        dt = self._active_dt()
        ctx = self._active_ctx()
        tree = self._active_tree()
        self._fold_all_and_rebuild(dt, ctx, tree)

    def action_toggle_fold(self) -> None:
        """Toggle fold/unfold for the selected row."""
        key = self._selected_key_val()
        if not key:
            return
        dt = self._active_dt()
        ctx = self._active_ctx()
        tree = self._active_tree()

        # History tab: project or sub-group rows
        if key.startswith('hproj::') or key.startswith('hgrp_'):
            self._toggle_and_rebuild(dt, ctx, tree, key)
            return

        # Current tab: project row
        if key.startswith(_PROJ):
            self._toggle_and_rebuild(dt, ctx, tree, key)
            return

        # Current tab: individual sub-row → no-op
        if key.startswith(_IND):
            return

        # Current tab: job group row → toggle group expand
        if len(self._group_ids.get(key, [])) > 1:
            self._toggle_and_rebuild(dt, ctx, tree, key)

    def action_refresh(self) -> None:
        self._load()

    def _visible_job_ids(self) -> List[str]:
        """Ordered list of job IDs currently visible in the table (for popup cycling)."""
        job_ids: List[str] = []
        for k in self._current_ctx.row_keys:
            if k.startswith(_PROJ):
                continue
            if k.startswith(_IND):
                job_ids.append(k.removeprefix(_IND))
            elif k not in self._current_ctx.expanded:
                job_ids.append(k)
        return job_ids

    def action_view_job(self) -> None:
        """Open the full JobInfoModal for the selected job."""
        jid = self._selected_job_id()
        if not jid:
            return
        job_ids = self._visible_job_ids()
        if jid not in job_ids:
            job_ids = self._group_ids.get(jid, [jid])
        current_idx = job_ids.index(jid) if jid in job_ids else 0

        sacct_ok = (getattr(self.app, '_cmd_sacct', True)
                    and getattr(self.app, '_feat_sacct_details', True))

        def _fetch() -> None:
            from speek.speek_max.slurm import fetch_job_details_and_log_path
            from speek.speek_max.log_scan import scan_log_incremental
            details, path = fetch_job_details_and_log_path(jid, sacct_fallback=sacct_ok)
            content, _ = scan_log_incremental(path, 0, 500) if path else (None, 0)
            from speek.speek_max.widgets.job_info_modal import JobInfoModal
            self.app.call_from_thread(
                lambda: self.app.push_screen(
                    JobInfoModal(jid, path, content, details, job_ids, current_idx)
                )
            )

        self.run_worker(_fetch, thread=True, group='job-info')

    def action_cancel_job(self) -> None:
        ids = self._all_jids_in_selection()
        if not ids:
            return

        def _after_select(selected_str: Optional[str]) -> None:
            if not selected_str:
                return
            selected_ids = [s.strip() for s in selected_str.split(',') if s.strip()]
            if selected_ids:
                self.post_message(self.CancelRequested(selected_ids))

        self.app.push_screen(CancelSelectModal(ids), _after_select)

    # ── Tab switching ──────────────────────────────────────────────────────

    def action_tab_current(self) -> None:
        try:
            self.query_one(_MYJOBS_TC, TabbedContent).active = 'tc-current'
        except Exception:
            pass
        try:
            self.query_one(_MYJOBS_DT, SpeekDataTable).focus()
        except Exception:
            pass

    def action_tab_history(self) -> None:
        try:
            self.query_one(_MYJOBS_TC, TabbedContent).active = 'tc-history'
        except Exception:
            pass
        self._load_history()
        try:
            self.query_one(_MYHIST_DT, SpeekDataTable).focus()
        except Exception:
            pass

    # ── History tab ────────────────────────────────────────────────────────

    def _load_history(self) -> None:
        if not getattr(self.app, '_cmd_sacct', True):
            return
        days = getattr(self.app, '_history_lookback_days', 7)

        def _worker():
            rows = fetch_history(days)
            self.app.call_from_thread(self._populate_history, rows)

        self.run_worker(_worker, thread=True, exclusive=True, group='myjobs-hist')

    @safe('MyJobs history')
    def _populate_history(self, rows: List[Tuple]) -> None:
        sig = str(len(rows)) + '|' + (rows[0][0] if rows else '')
        if sig == self._last_hist_sig:
            return
        self._last_hist_sig = sig

        dt = self.query_one(_MYHIST_DT, SpeekDataTable)
        empty = self.query_one('#myjobs-hist-empty', Static)

        if not rows:
            dt.clear()
            empty.update('No job history')
            empty.display = True
            return
        empty.display = False

        self._history_tree = self._build_history_tree(rows)
        self._rebuild(dt, self._history_ctx, self._history_tree)
