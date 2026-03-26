"""node_widget.py — Per-node GPU status panel."""
from __future__ import annotations

from collections import OrderedDict
from typing import List, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.widget import Widget
from textual.widgets import LoadingIndicator, Static

from speek.speek_max.slurm import parse_nodes
from speek.speek_max._utils import tc
from speek.speek_max.widgets.datatable import SpeekDataTable
from speek.speek_max.widgets.foldable_table import (
    FoldableTableMixin, FoldGroup, FoldMode, Leaf, TreeNode,
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
        self._ctx = self._init_ctx(renderer=self._render_cell, n_cols=6, name_col_width=20)
        self._last_rows: List[Tuple] = []
        self._tree: List[TreeNode] = []
        dt = self.query_one(SpeekDataTable)
        dt.zebra_stripes = True
        dt.add_column('Node',    width=20)
        dt.add_column('#N',      width=3)
        dt.add_column('Free',    width=4)
        dt.add_column('Total',   width=4)
        dt.add_column('State',   width=7)
        dt.add_column('Reason',  width=12)

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
        """Partition dividers → node leaves (or folds for multi-partition nodes)."""
        # Group by partition — a node in multiple partitions appears under each
        by_part: OrderedDict[str, List[Tuple]] = OrderedDict()
        for r in rows:
            parts_str = r[1]  # comma-separated partitions
            for p in parts_str.split(','):
                p = p.strip()
                if p:
                    by_part.setdefault(p, []).append(r)

        # Sort partitions by total GPUs descending (match queue/clusterbar order)
        sorted_parts = sorted(by_part.items(),
                              key=lambda kv: sum(r[4] for r in kv[1]), reverse=True)

        # Prune fold state
        valid_keys = {p for p, _ in sorted_parts}
        self._ctx.collapsed &= valid_keys

        tree: List[TreeNode] = []
        for part, items in sorted_parts:
            # Sort nodes: free desc, then name
            items.sort(key=lambda r: (-r[3], r[0]))
            # Aggregate partition stats
            agg_free = sum(r[3] for r in items)
            agg_total = sum(r[4] for r in items)
            agg_model = items[0][2] if items else '?'
            models = set(r[2] for r in items)
            if len(models) > 1:
                agg_model = ','.join(sorted(models))

            children = [
                Leaf(key=f'{r[0]}:{part}', data=r, indent=2)
                for r in items
            ]
            tree.append(FoldGroup(
                key=f'part::{part}',
                fold_key=part,
                data={
                    'part': part, 'model': agg_model,
                    'free': agg_free, 'total': agg_total,
                    'n_nodes': len(items),
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
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        c_error = tc(tv, 'text-error', 'red')
        state_color = {
            'idle': c_success, 'mixed': c_warning, 'alloc': c_error,
            'drain': c_error, 'down': c_error, 'maint': c_muted,
        }

        if isinstance(node, FoldGroup):
            # Partition divider with usage-proportional colored bar
            d = node.data
            total = d['total']
            free = d['free']
            used = total - free
            pct = used / total if total else 0.0

            if pct >= 1.0:
                bar_color = '#CC3333'
            elif pct >= 0.50:
                bar_color = tc(tv, 'warning', 'yellow')
            else:
                bar_color = tc(tv, 'success', 'green')

            w = self._ctx.name_col_width
            label = d['part']
            filled = max(1, int(round(pct * w))) if pct > 0 else 0
            empty_w = w - filled
            name_cell = Text()
            name_cell.append(f'│ {label}'[:filled].ljust(filled),
                             style=f'bold black on {bar_color}')
            if empty_w > 0:
                remaining = f'│ {label}'[filled:]
                name_cell.append(remaining.ljust(empty_w)[:empty_w],
                                 style='bold black on white')

            fc = c_success if free > 0 else c_error
            cells = [Text(' ') for _ in range(n_cols)]
            cells[0] = name_cell
            cells[1] = Text(str(d['n_nodes']), style=c_muted)
            cells[2] = Text(str(free), style=f'bold {fc}')
            cells[3] = Text(str(total), style=c_muted)
            cells[4] = Text('')
            cells[5] = Text('')
            return cells

        if isinstance(node, Leaf):
            r = node.data
            node_name, _parts, _model, free, total, state, reason = r
            sc = state_color.get(state, 'default')
            fc = c_success if free > 0 else c_error
            branch = '└' if node.is_last else '├'
            return [
                Text(f' {branch}── {node_name}'),
                Text(''),
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
