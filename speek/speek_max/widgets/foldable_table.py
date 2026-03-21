"""foldable_table.py — Unified tree-based table engine with dividers and folds.

Widgets provide:
  1. Column definitions (name, width)
  2. A tree of nodes (Divider, FoldGroup, Leaf, Spacer)
  3. A cell renderer callback

The engine handles:
  - Clearing and rebuilding the DataTable
  - Divider rows (│ + white bg)
  - Fold headers (▶/▼ + indented)
  - Expanding/collapsing state
  - Cursor save/restore
  - Row key tracking
  - Toggle/fold-all actions
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from rich.text import Text

from speek.speek_max.widgets.datatable import SpeekDataTable


# ── Fold semantics ────────────────────────────────────────────────────────

class FoldMode(Enum):
    COLLAPSED_SET = auto()  # default open; items in set are closed
    EXPANDED_SET  = auto()  # default closed; items in set are open


# ── Tree node types ───────────────────────────────────────────────────────

@dataclass
class Spacer:
    """Empty separator row."""
    key: str


@dataclass
class Divider:
    """Non-interactive section header (time zones, partition names).
    `extra_cells` maps column index → Text for aggregate info."""
    key: str
    label: str
    extra_cells: Dict[int, Text] = field(default_factory=dict)
    style: str = 'bold black on white'
    prefix: str = '│'


@dataclass
class Leaf:
    """Terminal data row. `data` is opaque — passed to the renderer.
    `is_last` is set automatically by the engine during emit."""
    key: str
    data: Any = None
    indent: int = 0
    is_last: bool = False


@dataclass
class FoldGroup:
    """Collapsible group header with children.
    `data` is opaque — passed to the renderer for the header row."""
    key: str
    fold_key: str
    data: Any = None
    children: List[Union['FoldGroup', Leaf, Spacer, Divider]] = field(default_factory=list)
    mode: FoldMode = FoldMode.COLLAPSED_SET
    indent: int = 0


TreeNode = Union[Spacer, Divider, Leaf, FoldGroup]

# Cell renderer type: (node, is_collapsed, n_cols) → list of Text cells
CellRenderer = Callable[[TreeNode, bool, int], List[Text]]


# ── Table context (mutable state) ─────────────────────────────────────────

@dataclass
class TableContext:
    """Mutable state for one DataTable."""
    renderer: Optional[CellRenderer] = None
    row_keys: List[str] = field(default_factory=list)
    collapsed: Set[str] = field(default_factory=set)
    expanded: Set[str] = field(default_factory=set)
    name_col_width: int = 22
    name_col_idx: int = 0
    n_cols: int = 0

    def is_collapsed(self, fold_key: str, mode: FoldMode) -> bool:
        if mode == FoldMode.EXPANDED_SET:
            return fold_key not in self.expanded
        return fold_key in self.collapsed

    def toggle(self, fold_key: str, mode: FoldMode) -> None:
        target = self.expanded if mode == FoldMode.EXPANDED_SET else self.collapsed
        target.symmetric_difference_update({fold_key})

    def fold_all(self, keys: Set[str], mode: FoldMode) -> None:
        target = self.expanded if mode == FoldMode.EXPANDED_SET else self.collapsed
        if mode == FoldMode.EXPANDED_SET:
            if target & keys:
                target -= keys
            else:
                target |= keys
        else:
            if keys - target:
                target |= keys
            else:
                target -= keys

    def prune(self, valid_keys: Set[str]) -> None:
        self.collapsed &= valid_keys
        self.expanded &= valid_keys


# ── Rebuild engine ────────────────────────────────────────────────────────

def _build_divider_cells(ctx: TableContext, node: Divider) -> List[Text]:
    cells = [Text(' ') for _ in range(ctx.n_cols)]
    label = f'{node.prefix} {node.label}'
    cells[ctx.name_col_idx] = Text(label.ljust(ctx.name_col_width), style=node.style)
    for idx, cell in node.extra_cells.items():
        if 0 <= idx < ctx.n_cols and idx != ctx.name_col_idx:
            cells[idx] = cell
    return cells


def _emit(dt: SpeekDataTable, ctx: TableContext, node: TreeNode) -> None:
    """Recursively emit rows for one tree node."""

    if isinstance(node, Spacer):
        dt.add_row(*[Text('') for _ in range(ctx.n_cols)], key=node.key)
        ctx.row_keys.append(node.key)
        return

    if isinstance(node, Divider):
        cells = _build_divider_cells(ctx, node)
        dt.add_row(*cells, key=node.key)
        ctx.row_keys.append(node.key)
        return

    if isinstance(node, Leaf):
        cells = ctx.renderer(node, False, ctx.n_cols)
        dt.add_row(*cells, key=node.key)
        ctx.row_keys.append(node.key)
        return

    if isinstance(node, FoldGroup):
        collapsed = ctx.is_collapsed(node.fold_key, node.mode)
        cells = ctx.renderer(node, collapsed, ctx.n_cols)
        dt.add_row(*cells, key=node.key)
        ctx.row_keys.append(node.key)
        if not collapsed:
            for i, child in enumerate(node.children):
                if isinstance(child, Leaf):
                    child.is_last = (i == len(node.children) - 1)
                _emit(dt, ctx, child)


def rebuild(dt: SpeekDataTable, ctx: TableContext, tree: List[TreeNode],
            *, batch_app=None) -> None:
    """Clear and rebuild a DataTable from a tree of nodes.

    Args:
        dt: The SpeekDataTable to rebuild.
        ctx: TableContext with renderer, state, and config.
        tree: List of tree nodes to emit.
        batch_app: If provided, wraps in `batch_app.batch_update()`.
    """
    ctx.row_keys = []
    if batch_app:
        with batch_app.batch_update():
            dt.clear()
            for node in tree:
                _emit(dt, ctx, node)
    else:
        dt.clear()
        for node in tree:
            _emit(dt, ctx, node)


# ── Cursor helpers ────────────────────────────────────────────────────────

def save_cursor(dt: SpeekDataTable) -> Optional[str]:
    try:
        if dt.row_count > 0:
            return str(dt.coordinate_to_cell_key(dt.cursor_coordinate)[0].value)
    except Exception:
        pass
    return None


def restore_cursor(dt: SpeekDataTable, ctx: TableContext,
                   saved_key: Optional[str]) -> None:
    if not saved_key:
        return
    try:
        dt.move_cursor(row=dt.get_row_index(saved_key))
        return
    except Exception:
        pass
    try:
        dt.move_cursor(row=ctx.row_keys.index(saved_key))
    except (ValueError, Exception):
        pass


def selected_key(dt: SpeekDataTable, ctx: TableContext) -> Optional[str]:
    try:
        if dt.row_count > 0:
            return str(dt.coordinate_to_cell_key(dt.cursor_coordinate)[0].value)
    except Exception:
        pass
    try:
        row = dt.cursor_row
        if 0 <= row < len(ctx.row_keys):
            return ctx.row_keys[row]
    except Exception:
        pass
    return None


# ── Tree query helpers ────────────────────────────────────────────────────

def find_node(tree: List[TreeNode], key: str) -> Optional[TreeNode]:
    """Find a node by key (recursive)."""
    for node in tree:
        if hasattr(node, 'key') and node.key == key:
            return node
        if isinstance(node, FoldGroup):
            found = find_node(node.children, key)
            if found:
                return found
    return None


def collect_fold_keys(tree: List[TreeNode],
                      mode: Optional[FoldMode] = None) -> Set[str]:
    """Collect all fold_keys from FoldGroup nodes."""
    keys: Set[str] = set()
    for node in tree:
        if isinstance(node, FoldGroup):
            if mode is None or node.mode == mode:
                keys.add(node.fold_key)
            keys |= collect_fold_keys(node.children, mode)
    return keys


# ── Convenience mixin (optional — widgets can use module functions directly)

class FoldableTableMixin:
    """Thin mixin that delegates to module-level functions for backward compat."""

    def _init_ctx(self, renderer: CellRenderer, n_cols: int, **kw) -> TableContext:
        return TableContext(renderer=renderer, n_cols=n_cols, **kw)

    def _rebuild(self, dt: SpeekDataTable, ctx: TableContext,
                 tree: List[TreeNode]) -> None:
        cursor = save_cursor(dt)
        rebuild(dt, ctx, tree, batch_app=getattr(self, 'app', None))
        restore_cursor(dt, ctx, cursor)

    def _toggle_and_rebuild(self, dt: SpeekDataTable, ctx: TableContext,
                            tree: List[TreeNode], key: str) -> None:
        node = find_node(tree, key)
        if isinstance(node, FoldGroup):
            ctx.toggle(node.fold_key, node.mode)
        self._rebuild(dt, ctx, tree)

    def _fold_all_and_rebuild(self, dt: SpeekDataTable, ctx: TableContext,
                              tree: List[TreeNode],
                              mode: Optional[FoldMode] = None) -> None:
        keys = collect_fold_keys(tree, mode)
        if mode == FoldMode.EXPANDED_SET:
            ctx.fold_all(keys, FoldMode.EXPANDED_SET)
        elif mode == FoldMode.COLLAPSED_SET:
            ctx.fold_all(keys, FoldMode.COLLAPSED_SET)
        else:
            # Toggle both sets
            ctx.fold_all(
                collect_fold_keys(tree, FoldMode.COLLAPSED_SET),
                FoldMode.COLLAPSED_SET,
            )
            ctx.fold_all(
                collect_fold_keys(tree, FoldMode.EXPANDED_SET),
                FoldMode.EXPANDED_SET,
            )
        self._rebuild(dt, ctx, tree)

    @staticmethod
    def _selected_key(dt: SpeekDataTable, ctx: TableContext) -> Optional[str]:
        return selected_key(dt, ctx)
