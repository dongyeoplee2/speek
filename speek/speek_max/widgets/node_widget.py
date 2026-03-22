"""node_widget.py — Per-node GPU status panel."""
from __future__ import annotations

from collections import OrderedDict
from typing import Dict, List, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.widget import Widget
from textual.widgets import LoadingIndicator, Static

from speek.speek_max.slurm import parse_nodes
from speek.speek_max._utils import tc
from speek.speek_max.widgets.datatable import SpeekDataTable
from speek.speek_max.widgets.foldable_table import (
    FoldableTableMixin, FoldGroup, FoldMode, Leaf,
    TreeNode, TableContext, _build_divider_cells, Divider,
)


class NodeWidget(FoldableTableMixin, Widget):
    """Per-node GPU status."""

    BORDER_TITLE = "Nodes"
    can_focus = True

    BINDINGS = [
        Binding('r', 'refresh', 'Refresh', show=True),
        Binding('v', 'toggle_fold', '▶/▼', show=True),
        Binding('V', 'fold_all', '', show=False),
    ]

    def compose(self) -> ComposeResult:
        """Compose the node status widget."""
        yield LoadingIndicator()
        yield Static('', id='node-empty', classes='empty-state')
        yield SpeekDataTable(id='node-dt', cursor_type='row')

    def on_mount(self) -> None:
        self._ctx = self._init_ctx(renderer=self._render_cell, n_cols=7, name_col_width=10)
        self._last_rows: List[Tuple] = []
        self._tree: List[TreeNode] = []
        dt = self.query_one(SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('Node',      width=10)
        dt.add_column('Partition', width=6)
        dt.add_column('GPU',       width=8)
        dt.add_column('Free',      width=4)
        dt.add_column('Total',     width=4)
        dt.add_column('State',     width=7)
        dt.add_column('Reason',    width=12)

    def on_show(self) -> None:
        self._load()
        if not hasattr(self, '_interval_started'):
            self._interval_started = True
            interval = getattr(self.app, '_node_refresh', 30)
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
        if not getattr(self.app, '_cmd_scontrol', True):
            try:
                self.query_one(LoadingIndicator).display = False
                e = self.query_one('#node-empty', Static)
                e.update('[dim]scontrol unavailable on this cluster[/dim]')
                e.display = True
            except Exception:
                pass
            return
        self.run_worker(self._fetch, thread=True, exclusive=True, group='nodes')

    def _fetch(self) -> None:
        from textual.worker import get_current_worker
        worker = get_current_worker()
        rows = parse_nodes()
        if not worker.is_cancelled:
            self.app.call_from_thread(self._update, rows)

    # ── Tree building ────────────────────────────────────────────────────────

    def _build_tree(self, rows: List[Tuple]) -> List[TreeNode]:
        """Convert node rows into a tree: single-partition nodes as Leaf,
        multi-partition nodes as FoldGroup with COLLAPSED_SET."""
        grouped: OrderedDict[str, List[Tuple]] = OrderedDict()
        for r in rows:
            node = r[0]
            grouped.setdefault(node, []).append(r)

        # Prune fold state
        self._ctx.collapsed &= set(grouped.keys())

        tree: List[TreeNode] = []
        for node, items in grouped.items():
            if len(items) == 1:
                r = items[0]
                key = f'{r[0]}:{r[1]}'
                tree.append(Leaf(key=key, data=r, indent=0))
            else:
                children = [
                    Leaf(key=f'{r[0]}:{r[1]}', data=r, indent=3)
                    for r in items
                ]
                # Aggregate info for the divider-style header
                all_parts = ','.join(r[1] for r in items)
                agg_model = items[0][2]
                agg_free = sum(r[3] for r in items)
                agg_total = sum(r[4] for r in items)
                state_prio = {'down': 0, 'drain': 1, 'alloc': 2, 'mixed': 3, 'maint': 4, 'idle': 5}
                agg_state = min((r[5] for r in items), key=lambda s: state_prio.get(s, 99))
                tree.append(FoldGroup(
                    key=f'div::{node}',
                    fold_key=node,
                    data={
                        'node': node, 'parts': all_parts, 'model': agg_model,
                        'free': agg_free, 'total': agg_total, 'state': agg_state,
                    },
                    children=children,
                    mode=FoldMode.COLLAPSED_SET,
                    indent=0,
                ))
        return tree

    def _render_cell(self, node: TreeNode, is_collapsed: bool, n_cols: int) -> List[Text]:
        """Render a single tree node to cells."""
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_secondary = tc(tv, 'text-secondary', 'default')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        c_error = tc(tv, 'text-error', 'red')
        state_color = {
            'idle': c_success, 'mixed': c_warning, 'alloc': c_error,
            'drain': c_error, 'down': c_error, 'maint': c_muted,
        }

        if isinstance(node, FoldGroup):
            # Multi-partition node divider
            d = node.data
            sc = state_color.get(d['state'], 'default')
            fc = c_success if d['free'] > 0 else c_error
            div = Divider(
                key=node.key, label=d['node'],
                extra_cells={
                    1: Text(d['parts'], style=f'dim {c_secondary}'),
                    2: Text(d['model'], style='dim'),
                    3: Text(str(d['free']), style=f'bold {fc}'),
                    4: Text(str(d['total']), style=c_muted),
                    5: Text(d['state'], style=f'bold {sc}'),
                    6: Text(' '),
                },
            )
            return _build_divider_cells(self._ctx, div)

        if isinstance(node, Leaf):
            r = node.data
            node_name, parts, model, free, total, state, reason = r
            sc = state_color.get(state, 'default')
            fc = c_success if free > 0 else c_error
            if node.indent > 0:
                # Child of multi-partition group
                return [
                    Text(f'  {node_name}', style=c_muted),
                    Text(parts, style=c_secondary),
                    Text(model),
                    Text(str(free), style=f'bold {fc}'),
                    Text(str(total), style=c_muted),
                    Text(state, style=f'bold {sc}'),
                    Text(reason, style=c_muted),
                ]
            else:
                # Single-partition node at top level
                return [
                    Text(node_name, style='bold'),
                    Text(parts, style=c_secondary),
                    Text(model),
                    Text(str(free), style=f'bold {fc}'),
                    Text(str(total), style=c_muted),
                    Text(state, style=f'bold {sc}'),
                    Text(reason, style=c_muted),
                ]

        return [Text('') for _ in range(n_cols)]

    def _update(self, rows: List[Tuple]) -> None:
        self._last_rows = rows
        self.query_one(LoadingIndicator).display = False
        empty = self.query_one('#node-empty', Static)
        dt = self.query_one(SpeekDataTable)

        if not rows:
            empty.update('No nodes reported by SLURM')
            empty.display = True
        else:
            empty.display = False

        self._tree = self._build_tree(rows)
        self._rebuild(dt, self._ctx, self._tree)

    # ── Actions ───────────────────────────────────────────────────────────────

    def action_toggle_fold(self) -> None:
        """Toggle fold/unfold for the selected node divider."""
        dt = self.query_one(SpeekDataTable)
        key = self._selected_key(dt, self._ctx)
        if not key:
            return
        self._toggle_and_rebuild(dt, self._ctx, self._tree, key)

    def action_fold_all(self) -> None:
        """Fold or unfold ALL multi-partition nodes."""
        dt = self.query_one(SpeekDataTable)
        self._fold_all_and_rebuild(dt, self._ctx, self._tree, FoldMode.COLLAPSED_SET)

    def action_refresh(self) -> None:
        self._load()
