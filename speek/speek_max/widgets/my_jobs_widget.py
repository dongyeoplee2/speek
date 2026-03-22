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

from speek.speek_max.slurm import (
    fetch_all_priorities, fetch_history, fetch_active_jobs_scontrol,
    fetch_my_jobs, get_job_log_path,
)
from speek.speek_max._utils import fmt_time, tc, safe, state_sym, state_badge, job_name_cell as _job_name_cell
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


def _parse_eta_secs(eta: str) -> int:
    """Parse ETA string like '~14h 30m' or '~3m' back to seconds."""
    import re
    s = eta.lstrip('~')
    h = m = 0
    mh = re.search(r'(\d+)h', s)
    mm = re.search(r'(\d+)m', s)
    if mh:
        h = int(mh.group(1))
    if mm:
        m = int(mm.group(1))
    return h * 3600 + m * 60


def _eta_range(etas: List[str]) -> str:
    """Return min~max ETA range string, properly sorted by duration."""
    if not etas:
        return ''
    parsed = [(e, _parse_eta_secs(e)) for e in etas]
    parsed.sort(key=lambda x: x[1])
    lo = parsed[0][0].lstrip('~')
    hi = parsed[-1][0].lstrip('~')
    if lo == hi or len(parsed) == 1:
        return lo
    return f'{lo}~{hi}'


def _compact_ids(jids: List[str]) -> str:
    """Compact job IDs into ranges: [12345,12346,12347,12350] → '12345-47,12350'."""
    if not jids:
        return ''
    nums: List[int] = []
    for j in jids:
        try:
            nums.append(int(j.split('_')[0]))
        except ValueError:
            return ','.join(jids[:5]) + (f'+{len(jids)-5}' if len(jids) > 5 else '')
    nums.sort()
    ranges: List[str] = []
    start = prev = nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
        else:
            ranges.append(_fmt_range(start, prev))
            start = prev = n
    ranges.append(_fmt_range(start, prev))
    return ','.join(ranges)


def _fmt_range(start: int, end: int) -> str:
    if start == end:
        return str(start)
    # Abbreviate: 12345-12347 → 12345-47
    s, e = str(start), str(end)
    common = 0
    for a, b in zip(s, e):
        if a == b:
            common += 1
        else:
            break
    suffix = e[common:] if common > 0 else e
    return f'{s}-{suffix}'


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
    for row in rows:
        jid, name, part, gpus, state, elapsed, eta, start = row[:8]
        submit = row[8] if len(row) > 8 else ''
        gpu_model = row[9] if len(row) > 9 else ''
        proj = _project_name(name)
        item = {'jid': jid, 'name': name, 'part': part, 'gpu': gpus,
                'state': state, 'elapsed': elapsed, 'eta': eta, 'start': start,
                'submit': submit, 'gpu_model': gpu_model}
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
        Binding('v',     'toggle_fold', '▶/▼',      show=True),
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
        dt.add_column('State',     width=5)
        dt.add_column('Elapsed',   width=7)
        dt.add_column('ETA',       width=5)
        dt.add_column('IDs',       width=14)
        self._log_col = dt.add_column('Log', width=32)
        self._current_ctx = self._init_ctx(renderer=self._render_current_cell, n_cols=8, name_col_width=22)
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
        shared = {
            'priorities': priorities, 'part_ranked': part_ranked,
            'part_rank_index': part_rank_index,
            'state_style': state_style, 'colors': colors,
        }

        for proj, items in projects.items():
            for item in items:
                self._job_data[item['jid']] = item

            # Group by submit datetime → individual jobs
            from collections import OrderedDict as _OD
            by_submit: Dict[str, List[dict]] = _OD()
            for item in items:
                raw = item.get('submit', '')
                # Format: strip year, e.g. "2026-03-22T14:30:00" → "03-22 14:30"
                if raw and raw not in ('N/A', 'Unknown') and len(raw) >= 16:
                    submit_label = raw[5:16].replace('T', ' ').replace('-', '/')
                elif raw and 'T' in raw:
                    submit_label = raw.split('T')[0]
                else:
                    submit_label = 'unknown'
                by_submit.setdefault(submit_label, []).append(item)

            submit_children: List[TreeNode] = []
            for submit_label, sub_items in by_submit.items():
                # Sort: running first, then pending
                sub_items.sort(key=lambda it: (0 if it['state'] == 'RUNNING' else 1))

                # Group by GPU model, then by state under each GPU
                by_gpu: Dict[str, List[dict]] = _OD()
                for item in sub_items:
                    gm = item.get('gpu_model', '') or item.get('part', '')
                    by_gpu.setdefault(gm, []).append(item)

                gpu_children: List[TreeNode] = []
                for gpu_label, gpu_items in by_gpu.items():
                    # Track all jids under this GPU group
                    all_jids = [it['jid'] for it in gpu_items]
                    first_jid = all_jids[0]
                    self._group_ids[first_jid] = all_jids
                    new_first_jids.append(first_jid)

                    # State-level folds under GPU (grouped by state, name = job IDs)
                    by_state: Dict[str, List[dict]] = _OD()
                    for it in gpu_items:
                        by_state.setdefault(it['state'], []).append(it)

                    is_mixed = len(by_state) > 1
                    if is_mixed:
                        # Mixed states: state-level folds under GPU
                        gpu_sub: List[TreeNode] = []
                        for st, st_items in by_state.items():
                            st_jids = [it['jid'] for it in st_items]
                            st_first = st_jids[0]
                            ids_label = _compact_ids(st_jids)
                            self._group_ids[st_first] = st_jids
                            job_leaves: List[TreeNode] = []
                            for item in st_items:
                                job_leaves.append(Leaf(
                                    key=f'ind::{item["jid"]}',
                                    data={'jid': item['jid'], 'group': item, **shared},
                                    indent=8,
                                ))
                            gpu_sub.append(FoldGroup(
                                key=f'st::{proj}::{submit_label}::{gpu_label}::{st}',
                                fold_key=f'{proj}::{submit_label}::{gpu_label}::{st}',
                                data={'state_label': st, 'ids_label': ids_label,
                                      'items': st_items, 'colors': colors, **shared},
                                children=job_leaves,
                                mode=FoldMode.EXPANDED_SET,
                                indent=6,
                            ))
                    else:
                        # Single state: job leaves directly under GPU
                        gpu_sub = []
                        for item in gpu_items:
                            jid = item['jid']
                            self._group_ids[jid] = [jid]
                            gpu_sub.append(Leaf(
                                key=f'ind::{jid}',
                                data={'jid': jid, 'group': item, **shared},
                                indent=6,
                            ))

                    # Mixed: default open (show state groups); single: default closed
                    gpu_mode = FoldMode.COLLAPSED_SET if is_mixed else FoldMode.EXPANDED_SET
                    gpu_children.append(FoldGroup(
                        key=f'gpu::{proj}::{submit_label}::{gpu_label}',
                        fold_key=f'{proj}::{submit_label}::{gpu_label}',
                        data={'gpu_label': gpu_label, 'items': gpu_items,
                              'colors': colors, **shared},
                        children=gpu_sub,
                        mode=gpu_mode,
                        indent=4,
                    ))

                if len(gpu_children) == 1 and isinstance(gpu_children[0], Leaf):
                    gpu_children[0].indent = 2
                    submit_children.append(gpu_children[0])
                else:
                    submit_children.append(FoldGroup(
                        key=f'submit::{proj}::{submit_label}',
                        fold_key=f'{proj}::{submit_label}',
                        data={'submit_label': submit_label, 'items': sub_items,
                              'colors': colors, **shared},
                        children=gpu_children,
                        mode=FoldMode.COLLAPSED_SET,
                        indent=2,
                    ))

            tree.append(FoldGroup(
                key=f'{_PROJ}{proj}',
                fold_key=proj,
                data={'proj': proj, 'items': items, 'colors': colors},
                children=submit_children,
                mode=FoldMode.COLLAPSED_SET,
                indent=0,
            ))

        self._new_first_jids = new_first_jids
        return tree

    def _render_current_cell(self, node: TreeNode, is_collapsed: bool, n_cols: int) -> List[Text]:
        """Render a Current tab tree node.

        Tree hierarchy: Project (COLLAPSED_SET)
          → Submit datetime (COLLAPSED_SET, has submit_label)
            → GPU model (EXPANDED_SET, has gpu_label)
              → State group (Leaf, has state_label + ids_label)
        """
        if isinstance(node, FoldGroup):
            if 'proj' in node.data:
                return self._render_project_header(node, is_collapsed)
            if 'submit_label' in node.data:
                return self._render_submit_header(node, is_collapsed)
            if 'gpu_label' in node.data:
                return self._render_gpu_header(node, is_collapsed)
            if 'state_label' in node.data:
                return self._render_state_header(node, is_collapsed)

        if isinstance(node, Leaf):
            return self._render_individual_job(node)

        return [Text('') for _ in range(n_cols)]

    def _render_project_header(self, node: FoldGroup, is_collapsed: bool) -> List[Text]:
        d = node.data
        proj = d['proj']
        items = d['items']
        icon = '▶' if is_collapsed else '▼'
        return self._aggregate_cells(f'{icon} {proj}', items, d['colors'],
                                      indent_style='bold', show_state=is_collapsed)

    def _render_submit_header(self, node: FoldGroup, is_collapsed: bool) -> List[Text]:
        """Render submit-datetime fold header with aggregates."""
        d = node.data
        label = d['submit_label']
        icon = '▶' if is_collapsed else '▼'
        items = d.get('items', [])
        return self._aggregate_cells(f'  {icon} {label}', items, d['colors'],
                                      indent_style='bold', show_state=is_collapsed)

    def _render_gpu_header(self, node: FoldGroup, is_collapsed: bool) -> List[Text]:
        """Render GPU model fold header. Hide state when unfolded (children show it)."""
        d = node.data
        label = d['gpu_label']
        icon = '▶' if is_collapsed else '▼'
        items = d.get('items', [])
        c_secondary = d['colors'][1]
        return self._aggregate_cells(f'    {icon} {label}', items, d['colors'],
                                      indent_style=c_secondary, show_state=is_collapsed)

    def _render_state_header(self, node: FoldGroup, is_collapsed: bool) -> List[Text]:
        """Render a state-group fold: shows job IDs as name, single state badge."""
        d = node.data
        colors = d['colors']
        c_muted, c_secondary, _, _, _ = colors
        items = d.get('items', [])
        ids_label = d.get('ids_label', '')
        n_jobs = len(items)
        n_gpu = sum(int(it.get('gpu', 0)) for it in items if str(it.get('gpu', '')).isdigit())
        state = d['state_label']
        badge = state_badge(state)
        max_secs = max((self._parse_elapsed(it.get('elapsed', '')) for it in items), default=0)
        elapsed_str = self._fmt_elapsed(max_secs)
        icon = '▶' if is_collapsed else '▼'
        etas = [it.get('eta', '') for it in items if it.get('eta')]
        eta_str = _eta_range(etas)
        c_warning = colors[3]
        return [
            Text(f'      {icon} {ids_label}', style='default'),
            Text(str(n_jobs), style='bold'),
            Text(str(n_gpu) if n_gpu else '', style=f'bold {c_secondary}'),
            badge,
            Text(elapsed_str, style=c_muted),
            Text(eta_str, style=f'{c_warning} italic') if eta_str else Text(''),
            Text(ids_label, style=c_muted),
            Text(''),  # Log
        ]

    @staticmethod
    def _iter_leaves(node: FoldGroup):
        """Yield all leaf descendants."""
        for child in node.children:
            if isinstance(child, FoldGroup):
                yield from MyJobsWidget._iter_leaves(child)
            elif isinstance(child, Leaf):
                yield child

    def _aggregate_cells(self, label: str, items: List[dict], colors: tuple,
                         indent_style: str = 'bold',
                         show_state: bool = True) -> List[Text]:
        """Build a full 10-column row with aggregated data from items.

        Columns: Name, #J, #G, State(badge), Elapsed, Ago, ETA, Rank, IDs, Log
        """
        c_muted, c_secondary, c_success, c_warning, _ = colors
        n_jobs = len(items)
        from speek.speek_max._utils import STATE_SYMBOL, STATE_BG
        n_run = sum(1 for it in items if it.get('state') == 'RUNNING')
        n_pend = sum(1 for it in items if it.get('state') == 'PENDING')
        n_gpu = sum(int(it.get('gpu', 0)) for it in items if str(it.get('gpu', '')).isdigit())
        gpu_cell = Text(str(n_gpu) if n_gpu else '', style=f'bold {c_secondary}')
        # State badge: only shown when folded (show_state=True)
        if not show_state:
            badge = Text('')
        elif n_run and n_pend:
            badge = Text()
            badge.append(f'{STATE_SYMBOL["RUNNING"]}{n_run}', style=f'bold {c_success}')
            badge.append(' ')
            badge.append(f'{STATE_SYMBOL["PENDING"]}{n_pend}', style=f'bold {c_warning}')
        elif n_run:
            badge = state_badge('RUNNING')
        elif n_pend:
            badge = state_badge('PENDING')
        else:
            states = [it.get('state', '') for it in items]
            badge = state_badge(states[0]) if states else Text('')
        # Max elapsed
        max_secs = max((self._parse_elapsed(it.get('elapsed', '')) for it in items), default=0)
        elapsed_str = self._fmt_elapsed(max_secs)
        # ETA: min~max range from pending items
        etas = [it.get('eta', '') for it in items if it.get('eta')]
        eta_str = _eta_range(etas)
        # IDs
        jids = [it.get('jid', '') for it in items if it.get('jid')]
        ids_str = jids[0] if len(jids) == 1 else f'{jids[0]}+{len(jids)-1}' if jids else ''
        return [
            Text(label, style=indent_style),
            Text(str(n_jobs), style=f'bold {c_muted}'),
            gpu_cell,
            badge,
            Text(elapsed_str, style=c_muted),
            Text(eta_str, style=f'{c_warning} italic') if eta_str else Text(''),
            Text(ids_str, style=c_muted),
            Text(''),  # Log
        ]

    def _collect_items(self, node: FoldGroup) -> List[dict]:
        """Collect job item dicts from all leaf descendants."""
        items = []
        for leaf in self._iter_leaves(node):
            jid = leaf.data.get('jid', '') if leaf.data else ''
            it = self._job_data.get(jid)
            if it:
                items.append(it)
        return items

    def _render_part_header(self, node: FoldGroup, is_collapsed: bool) -> List[Text]:
        """Render partition fold header with aggregates."""
        d = node.data
        colors = d['colors']
        c_secondary = colors[1]
        part = d['part_label']
        icon = '▶' if is_collapsed else '▼'
        items = self._collect_items(node)
        return self._aggregate_cells(f'      {icon} {part}', items, colors, indent_style=c_secondary)

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
        # Show submit time as sub-group label
        g_submit = g.get('submit', '')
        start_label = g_submit[:16] if g_submit and g_submit not in ('N/A', 'Unknown') else g['name']
        return [
            Text(f'  {fold_icon}{start_label}', style='default'),
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
        colors = d['colors']
        c_muted, c_secondary, _, c_warning, c_error = colors

        it = self._job_data.get(jid, g)
        state = it.get('state', g['state'])
        eta = it.get('eta', '')
        has_oom = jid in self._oom_jobs
        badge = state_badge('OUT_OF_MEMORY') if has_oom else state_badge(state)
        hint = self._log_hints.get(jid, '')
        hint_style = f'bold {c_error}' if has_oom else c_muted
        indent = ' ' * max(0, node.indent - 2)
        branch = '└' if node.is_last else '├'
        return [
            _job_name_cell(f'{indent}{branch}── ', it.get('name', jid), jid, c_muted, single=(node.indent <= 4)),
            Text('1', style=f'bold {c_muted}'),
            Text(it.get('gpu', g['gpu']), style='bold'),
            badge,
            Text(it.get('elapsed', g['elapsed']), style=c_muted),
            Text(eta, style=f'{c_warning} italic') if eta else Text(''),
            Text(jid, style=c_muted),
            Text(hint[:40] if hint else '', style=hint_style),
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
            # Time divider based on actual date of most recent activity
            activity = _latest_activity(items)
            try:
                from datetime import datetime as _dt
                act_dt = _dt.strptime(activity.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S')
                dk = act_dt.strftime('%Y-%m-%d')
                now = _dt.now()
                delta = (now.date() - act_dt.date()).days
                weekday = act_dt.strftime('%a')
                short = act_dt.strftime('%m/%d')
                if delta == 0:
                    dlabel = f'Today  {short}'
                elif delta == 1:
                    dlabel = f'Yesterday  {short}'
                elif delta < 7:
                    dlabel = f'{weekday}  {short}'
                elif delta < 30:
                    dlabel = f'~{delta // 7}w ago  {short}'
                else:
                    dlabel = f'~{delta // 30}m ago  {short}'
            except Exception:
                dk = ''
                dlabel = 'Unknown'
            if dk != current_zone:
                if current_zone:
                    tree.append(Spacer(key=f'{_HIST_DIV_PREFIX}{div_counter}_sp'))
                current_zone = dk
                tree.append(Divider(
                    key=f'{_HIST_DIV_PREFIX}{div_counter}',
                    label=dlabel,
                ))
                div_counter += 1

            n = len(items)
            if n > 1:
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

                # If only one sub-group, skip the sub-group fold (redundant)
                if len(sub_groups) == 1:
                    proj_children = [
                        Leaf(key=f'hist_{item["jid"]}', data=item, indent=4)
                        for item in sub_groups[0]
                    ]
                else:
                    proj_children: List[TreeNode] = []
                    for sg in sub_groups:
                        sg_n = len(sg)
                        first = sg[0]
                        if sg_n > 1:
                            sg_key = f'hgrp_{first["jid"]}'
                            ind_children = [
                                Leaf(key=f'hist_{item["jid"]}', data=item, indent=6)
                                for item in sg
                            ]
                            proj_children.append(FoldGroup(
                                key=sg_key,
                                fold_key=sg_key,
                                data={'items': sg, 'first': first},
                                children=ind_children,
                                mode=FoldMode.COLLAPSED_SET,
                                indent=4,
                            ))
                        else:
                            proj_children.append(Leaf(
                                key=f'hist_{first["jid"]}',
                                data=first,
                                indent=4,
                            ))

                tree.append(FoldGroup(
                    key=f'hproj::{proj}',
                    fold_key=proj,
                    data={'proj': proj, 'items': items},
                    children=proj_children,
                    mode=FoldMode.COLLAPSED_SET,
                    indent=2,
                ))
            else:
                # Single-item project — just a leaf under the divider (show name)
                first = items[0]
                tree.append(Leaf(
                    key=f'hist_{first["jid"]}',
                    data=first,
                    indent=2,
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
            h_start = item.get('start', '')
            h_ago = fmt_time(h_start) if h_start and h_start not in ('N/A', 'Unknown') else '—'
            indent = ' ' * max(0, node.indent - 2)
            if node.indent >= 4:
                # Job under a group — show job ID (or name+id if single)
                branch = '└' if node.is_last else '├'
                return [
                    _job_name_cell(f'{indent}{branch}── ', item['name'], item['jid'], c_muted, single=False),
                    Text('', style=c_muted),
                    Text(item['gpu'], style=c_muted),
                    Text(item['part'], style=c_muted),
                    state_badge(item['state']),
                    Text(item['elapsed'], style=c_muted),
                    Text(h_ago, style=c_muted),
                    Text(item['jid'], style=c_muted),
                ]
            else:
                # Single job (no grouping) — show name + dimmed id
                return [
                    _job_name_cell('  ', item['name'], item['jid'], c_muted, single=True),
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
            Text(f'  {icon} {proj}', style='bold'),
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
            Text(f'    {icon} {first["name"]}', style=''),
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
        dt = self.query_one(SpeekDataTable)
        if not rows:
            empty.update(f'No active or pending jobs for {self.user}')
            empty.display = True
            stats_bar.display = False
            dt.display = False
        else:
            empty.display = False
            dt.display = True

        # Always post running count so header stays accurate
        self.post_message(self.RunningCount(n_run, n_pend))

        if sig == self._last_myjobs_sig:
            self._last_rows = rows
            self._last_priorities = priorities or {}
            return
        self._last_myjobs_sig = sig
        self._last_rows = rows
        self._last_priorities = priorities or {}

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

        all_jids = list(self._job_data.keys())
        uncached = [j for j in all_jids if j not in self._log_hints]
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
        # Fold group keys — not individual jobs
        if key.startswith((_PROJ, 'submit::', 'gpu::', 'st::')):
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

        # Current tab: project, submit-date, or partition row
        if key.startswith(_PROJ) or key.startswith('submit::') or key.startswith('gpu::') or key.startswith('st::'):
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
        days = getattr(self.app, '_history_lookback_days', 7)

        def _worker():
            if getattr(self.app, '_cmd_sacct', True):
                # Level 1: sacct (full history)
                rows = fetch_history(days)
            elif getattr(self.app, '_cmd_scontrol', True):
                # Level 2: scontrol active jobs + transition cache
                from speek.speek_max.event_watcher import load_fallback_history
                active = fetch_active_jobs_scontrol()
                cached = load_fallback_history(user=self.user, days=days)
                seen = set()
                rows = []
                for row in active:
                    if row[0] not in seen:
                        seen.add(row[0])
                        rows.append(row)
                for row in cached:
                    if row[0] not in seen:
                        seen.add(row[0])
                        rows.append(row)
            else:
                # Level 3: transition cache only
                from speek.speek_max.event_watcher import load_fallback_history
                rows = load_fallback_history(user=self.user, days=days)
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
