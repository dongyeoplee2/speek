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
from speek.speek_max._utils import tc
from speek.speek_max.widgets.datatable import SpeekDataTable


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
_LBL_PROJ_OPEN   = '▼ Project'
_LBL_PROJ_CLOSED = '▶ Project'
_LBL_JOBS_OPEN   = '▼ Jobs'
_LBL_JOBS_CLOSED = '▶ Jobs'


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
    for jid, name, part, gpus, state, elapsed, eta in rows:
        proj = _project_name(name)
        item = {'jid': jid, 'name': name, 'part': part, 'gpu': gpus,
                'state': state, 'elapsed': elapsed, 'eta': eta}
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


class MyJobsWidget(Widget):
    """Current user's running + pending jobs grouped by project."""

    BORDER_TITLE = "My Jobs"
    can_focus = True

    BINDINGS = [
        Binding('d',     'view_job',    'Details',  show=True),
        Binding('enter', 'view_job',   'Details',  show=False),
        Binding('v',     'toggle_fold', '▶/▼',     show=True),
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

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def __init__(self, user: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.user = user

    def compose(self) -> ComposeResult:
        """Compose the my-jobs widget."""
        yield LoadingIndicator()
        yield Static('', id='myjobs-empty', classes='empty-state')
        yield SpeekDataTable(id='myjobs-dt', cursor_type='row', show_cursor=True)
        yield Static('', id='myjobs-stats')

    def on_mount(self) -> None:
        self._row_keys: List[str] = []        # maps dt row index → key (proj:: or jid or ind::jid)
        self._collapsed: set[str] = set()     # collapsed project names
        self._expanded_groups: set[str] = set()  # first_jids of expanded groups
        self._log_hints: Dict[str, str] = {}  # first_jid → last log line
        self._group_ids: Dict[str, List[str]] = {}  # first_jid → all jids in group
        self._job_data: Dict[str, dict] = {}  # jid → individual item dict
        self._last_rows: List[Tuple] = []
        self._last_priorities: Dict = {}
        dt = self.query_one(SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('#',         width=3)
        dt.add_column('Name',      width=18)
        dt.add_column('Partition', width=10)
        dt.add_column('GPU',       width=6)
        dt.add_column('State',     width=9)
        dt.add_column('Elapsed',   width=9)
        dt.add_column('ETA',       width=10)
        dt.add_column('Rank',      width=7)
        dt.add_column('IDs',       width=16)
        self._log_col = dt.add_column('Log', width=32)
        self._load()
        interval = getattr(self.app, '_queue_refresh', 5)
        self._refresh_timer = self.set_interval(interval, self._load)

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

    def _project_header_row(
        self, proj: str, items: List[dict], collapsed: bool,
        c_muted: str, c_secondary: str, c_success: str, c_warning: str,
    ) -> tuple:
        indicator = '▶' if collapsed else '▼'
        total = sum(len(it.get('ids', [it])) for it in items)
        n_run  = sum(1 for it in items if it['state'] == 'RUNNING')
        n_pend = sum(1 for it in items if it['state'] == 'PENDING')
        n_gpu  = sum(int(it['gpu']) for it in items if it['gpu'].isdigit())
        state_t = Text()
        if n_run:
            state_t.append(str(n_run), style=f'bold {c_success}')
            state_t.append('R ', style=c_muted)
        if n_pend:
            state_t.append(str(n_pend), style=f'bold {c_warning}')
            state_t.append('P', style=c_muted)
        return (
            Text(str(total), style=f'bold {c_secondary}'),
            Text(f'{indicator} {proj}', style='bold'),
            Text('', style=c_muted),
            Text(str(n_gpu) if n_gpu else '', style=f'bold {c_secondary}'),
            state_t,
            Text('', style=c_muted),
            Text('', style=c_muted),
            Text('', style=c_muted),
            Text('', style=c_muted),
            Text('', style=c_muted),
        )

    def _add_project_rows(
        self, dt: SpeekDataTable, proj: str, items: List[dict],
        priorities: Dict, part_ranked: Dict, part_rank_index: Dict,
        new_first_jids: List[str], state_style: Dict, colors: tuple,
    ) -> None:
        c_muted, c_secondary, c_success, c_warning, _ = colors
        collapsed = proj in self._collapsed
        hdr = self._project_header_row(
            proj, items, collapsed, c_muted, c_secondary, c_success, c_warning
        )
        proj_key = f'{_PROJ}{proj}'
        dt.add_row(*hdr, key=proj_key)
        self._row_keys.append(proj_key)
        if collapsed:
            return
        for item in items:
            self._job_data[item['jid']] = item
        for g in _aggregate_within(items):
            first_jid = g['ids'][0]
            self._group_ids[first_jid] = g['ids']
            count = len(g['ids'])
            expanded = first_jid in self._expanded_groups and count > 1
            fold_icon = '▼ ' if expanded else ('▶ ' if count > 1 else '  ')
            ids_str = first_jid if count == 1 else f'{first_jid}+{count - 1}'
            rank = self._rank_cell(g, priorities, part_ranked,
                                   part_rank_index, c_warning, c_muted)
            hint = self._log_hints.get(first_jid, '')
            dt.add_row(
                Text(str(count), style=f'bold {c_muted}'),
                Text(f'  {fold_icon}{g["name"]}', style='default'),
                Text(g['part'], style=c_secondary),
                Text(g['gpu'], style='bold'),
                Text(g['state'], style=state_style.get(g['state'], f'dim {c_muted}')),
                Text(g['elapsed'], style=c_muted),
                Text(g['eta'], style=f'{c_warning} italic') if g['eta'] else Text(''),
                rank,
                Text(ids_str, style=c_muted),
                Text(hint[:32], style=c_muted),
                key=first_jid,
            )
            self._row_keys.append(first_jid)
            new_first_jids.append(first_jid)
            if expanded:
                self._add_individual_rows(dt, g, state_style, colors)

    def _add_individual_rows(
        self, dt: SpeekDataTable, g: dict, state_style: Dict, colors: tuple,
    ) -> None:
        c_muted, c_secondary, _, c_warning, _ = colors
        for jid in g['ids']:
            it = self._job_data.get(jid, g)
            state = it.get('state', g['state'])
            eta = it.get('eta', '')
            dt.add_row(
                Text('', style=c_muted),
                Text(f'    ↳ {jid}', style=c_muted),
                Text(it.get('part', g['part']), style=c_secondary),
                Text(it.get('gpu', g['gpu']), style='bold'),
                Text(state, style=state_style.get(state, f'dim {c_muted}')),
                Text(it.get('elapsed', g['elapsed']), style=c_muted),
                Text(eta, style=f'{c_warning} italic') if eta else Text(''),
                Text('', style=c_muted),
                Text(jid, style=c_muted),
                Text('', style=c_muted),
                key=f'ind::{jid}',
            )
            self._row_keys.append(f'ind::{jid}')

    def _update(
        self, sig: frozenset, rows: List[Tuple], priorities: Optional[Dict],
        projects: Dict, part_ranked: Dict, part_rank_index: Dict,
        stats_text: Optional[Text], n_run: int, n_pend: int, colors: tuple,
    ) -> None:
        if sig == getattr(self, '_last_myjobs_sig', None):
            self._last_rows = rows
            self._last_priorities = priorities or {}
            return
        self._last_myjobs_sig = sig
        self._last_rows = rows
        self._last_priorities = priorities or {}

        _, _, c_success, c_warning, c_error = colors
        state_style = {
            'RUNNING': f'bold {c_success}',
            'PENDING': c_warning,
            'FAILED':  f'bold {c_error}',
        }

        dt    = self.query_one(SpeekDataTable)
        empty = self.query_one('#myjobs-empty', Static)
        empty.display = not rows
        if not rows:
            empty.update(f'No active or pending jobs for {self.user}')

        self._collapsed &= set(projects.keys())

        # Evict stale log hints and per-job data
        live_jids = {it['jid'] for items in projects.values() for it in items}
        self._log_hints = {k: v for k, v in self._log_hints.items() if k in live_jids}
        self._job_data = {}
        self._row_keys = []
        self._group_ids = {}
        new_first_jids: List[str] = []

        with self.app.batch_update():
            dt.clear()
            for proj, items in projects.items():
                self._add_project_rows(
                    dt, proj, items, priorities,
                    part_ranked, part_rank_index,
                    new_first_jids, state_style, colors,
                )

        self._expanded_groups &= set(self._group_ids.keys())
        self.query_one(LoadingIndicator).display = False

        stats_bar = self.query_one('#myjobs-stats', Static)
        if stats_text is not None:
            stats_bar.update(stats_text)
            stats_bar.display = True
        else:
            stats_bar.display = False

        self.post_message(self.RunningCount(n_run, n_pend))

        uncached = [j for j in new_first_jids if j not in self._log_hints]
        if uncached:
            self.run_worker(
                lambda jids=uncached: self._fetch_log_hints(jids),
                thread=True, group='log-hints',
            )

    def _fetch_log_hints(self, jids: List[str]) -> Dict[str, str]:
        from speek.speek_max.log_scan import extract_hint
        results: Dict[str, str] = {}
        for jid in jids:
            path = get_job_log_path(jid)
            results[jid] = (extract_hint(path) or '') if path else ''
        return results

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.state != WorkerState.SUCCESS:
            return
        if event.worker.group == 'log-hints':
            self._log_hints.update(event.worker.result or {})
            self._apply_log_hints()

    def _apply_log_hints(self) -> None:
        dt = self.query_one(SpeekDataTable)
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        for jid, hint in self._log_hints.items():
            try:
                dt.update_cell(jid, self._log_col, Text(hint[:32], style=c_muted))
            except Exception:
                pass

    # ── Binding label updates ──────────────────────────────────────────────────

    def _update_fold_labels(self) -> None:
        key = self._selected_key()
        # z: show whether the current project is folded or unfolded
        if key and key.startswith(_PROJ):
            proj = key[len(_PROJ):]
            z_desc = _LBL_PROJ_CLOSED if proj in self._collapsed else _LBL_PROJ_OPEN
        else:
            z_desc = _LBL_PROJ_OPEN
        # v: show whether the current group is expanded or collapsed
        if key and not key.startswith(_PROJ) and not key.startswith(_IND):
            has_multi = len(self._group_ids.get(key, [])) > 1
            v_desc = _LBL_JOBS_OPEN if (has_multi and key in self._expanded_groups) else _LBL_JOBS_CLOSED
        else:
            v_desc = _LBL_JOBS_CLOSED
        self._bindings.bind('z', 'fold_project', z_desc, show=True)
        self._bindings.bind('v', 'unfold_group', v_desc, show=True)
        self.refresh_bindings()

    @on(DataTable.RowHighlighted)
    def _on_row_highlighted(self, _event: DataTable.RowHighlighted) -> None:
        self._update_fold_labels()

    # ── Selection helpers ──────────────────────────────────────────────────────

    def _active_dt(self) -> SpeekDataTable:
        return self.query_one(_MYJOBS_DT, SpeekDataTable)

    def _active_row_keys(self) -> List[str]:
        return self._row_keys

    def _selected_key(self) -> Optional[str]:
        dt = self._active_dt()
        keys = self._active_row_keys()
        if dt.row_count == 0:
            return None
        try:
            return keys[dt.cursor_row]
        except (IndexError, AttributeError):
            return None

    def _selected_job_id(self) -> Optional[str]:
        key = self._selected_key()
        if not key:
            return None
        # History tab keys
        if key.startswith('hist_'):
            return key[5:]
        if key.startswith('hproj::') or key.startswith(_HIST_DIV_PREFIX):
            return None
        # Current tab keys
        if key.startswith(_IND):
            return key.removeprefix(_IND)
        if key.startswith(_PROJ):
            keys = self._active_row_keys()
            dt = self._active_dt()
            for i in range(dt.cursor_row + 1, len(keys)):
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
        for k in self._row_keys:
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
        key = self._selected_key()
        if not key:
            return []
        if key.startswith(_IND):
            return [key.removeprefix(_IND)]
        if key.startswith(_PROJ):
            return self._jids_in_project(key)
        return self._group_ids.get(key, [key])

    # ── Actions ───────────────────────────────────────────────────────────────

    def _active_tab(self) -> str:
        try:
            return self.query_one(_MYJOBS_TC, TabbedContent).active.removeprefix('tc-')
        except Exception:
            return 'current'

    def action_toggle_fold(self) -> None:
        """Toggle fold/unfold for the selected row — works for both tabs."""
        key = self._selected_key()
        if not key:
            return

        # History tab project rows
        if key.startswith('hproj::'):
            proj = key[7:]
            if proj in self._hist_collapsed:
                self._hist_collapsed.discard(proj)
            else:
                self._hist_collapsed.add(proj)
            self._last_hist_sig = ''  # force re-render
            self._load_history()
            return

        # Current tab: project row
        if key.startswith(_PROJ):
            proj = key[len(_PROJ):]
            if proj in self._collapsed:
                self._collapsed.discard(proj)
            else:
                self._collapsed.add(proj)
            self._rebuild_from_last()
            try:
                self.query_one(_MYJOBS_DT, SpeekDataTable).move_cursor(
                    row=self._row_keys.index(key))
            except Exception:
                pass
            return

        # Current tab: individual sub-row → no-op
        if key.startswith(_IND):
            return

        # Current tab: job group row → toggle group expand
        if len(self._group_ids.get(key, [])) > 1:
            if key in self._expanded_groups:
                self._expanded_groups.discard(key)
            else:
                self._expanded_groups.add(key)
            self._rebuild_from_last()
            try:
                self.query_one(_MYJOBS_DT, SpeekDataTable).move_cursor(
                    row=self._row_keys.index(key))
            except Exception:
                pass

    def action_refresh(self) -> None:
        self._load()

    def _visible_job_ids(self) -> List[str]:
        """Ordered list of job IDs currently visible in the table (for popup cycling)."""
        job_ids: List[str] = []
        for k in self._row_keys:
            if k.startswith(_PROJ):
                continue
            if k.startswith(_IND):
                job_ids.append(k.removeprefix(_IND))
            elif k not in self._expanded_groups:
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

    def _populate_history(self, rows: List[Tuple]) -> None:
        import re as _re

        sig = str(len(rows)) + '|' + (rows[0][0] if rows else '')
        if sig == self._last_hist_sig:
            return
        self._last_hist_sig = sig

        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        c_error = tc(tv, 'text-error', 'red')

        dt = self.query_one(_MYHIST_DT, SpeekDataTable)
        empty = self.query_one('#myjobs-hist-empty', Static)

        if not rows:
            dt.clear()
            empty.update('No job history')
            empty.display = True
            return
        empty.display = False

        state_style = {
            'RUNNING': f'bold {c_success}',
            'PENDING': c_warning,
            'COMPLETED': c_muted,
            'FAILED': f'bold {c_error}',
            'TIMEOUT': f'bold {c_error}',
            'CANCELLED': f'dim {c_error}',
            'OUT_OF_MEMORY': f'bold {c_error}',
        }

        # Group by project name (same logic as Current tab)
        projects: OrderedDict[str, list] = OrderedDict()
        for r in rows:
            jid, name, part = r[0], r[1], r[2]
            start, elapsed, state = r[3], r[4], r[5]
            gpu_str = r[7] if len(r) > 7 else ''
            gpu = ''
            m = _re.search(r'gres/gpu(?::([^:,]+))?(?::(\d+)|=(\d+))', gpu_str)
            if m:
                gpu = f'{m.group(1) or "gpu"}:{m.group(2) or m.group(3) or "?"}'
            # Use name base for project grouping
            proj = _name_base(name) or name
            projects.setdefault(proj, []).append({
                'jid': jid, 'name': name, 'part': part,
                'state': state, 'elapsed': elapsed, 'start': start, 'gpu': gpu,
            })

        self._hist_row_keys = []
        with self.app.batch_update():
            dt.clear()
            current_zone = -1
            div_counter = 0

            for proj, items in projects.items():
                # Time divider
                zone = _hist_zone_idx(items[0]['start'])
                if zone != current_zone:
                    current_zone = zone
                    label = _HIST_TIME_ZONES[zone][1]
                    div_cells = [Text(f'── {label} ', style='dim italic')] + [Text('')] * 6
                    key = f'{_HIST_DIV_PREFIX}{div_counter}'
                    dt.add_row(*div_cells, key=key)
                    self._hist_row_keys.append(key)
                    div_counter += 1

                n = len(items)
                collapsed = proj in self._hist_collapsed

                # Project header row (same style as Current tab)
                if n > 1:
                    fold = '▶' if collapsed else '▼'
                    n_ok = sum(1 for i in items if i['state'] == 'COMPLETED')
                    n_fail = sum(1 for i in items if i['state'] in ('FAILED', 'TIMEOUT', 'OUT_OF_MEMORY'))
                    n_run = sum(1 for i in items if i['state'] == 'RUNNING')
                    n_pend = sum(1 for i in items if i['state'] == 'PENDING')
                    # State summary like Current tab: 2C 1F 1R
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

                    proj_key = f'hproj::{proj}'
                    dt.add_row(
                        Text(str(n), style=f'bold {c_muted}'),
                        Text(f'{fold} {proj[:16]}', style='bold'),
                        Text(items[0]['part'][:10], style=c_muted),
                        Text('', style=c_muted),
                        state_t,
                        Text('', style=c_muted),
                        Text('', style=c_muted),
                        key=proj_key,
                    )
                    self._hist_row_keys.append(proj_key)
                    if collapsed:
                        continue

                # Individual job rows
                for item in items:
                    ss = state_style.get(item['state'], c_muted)
                    dt.add_row(
                        Text('', style=c_muted),
                        Text(item['name'][:18]),
                        Text(item['part'][:10], style=c_muted),
                        Text(item['gpu'][:6], style=c_muted),
                        Text(item['state'][:9], style=ss),
                        Text(item['elapsed'][:9], style=c_muted),
                        Text(item['jid'], style=c_muted),
                        key=f'hist_{item["jid"]}',
                    )
                    self._hist_row_keys.append(f'hist_{item["jid"]}')
