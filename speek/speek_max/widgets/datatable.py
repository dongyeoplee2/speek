"""datatable.py — SpeekDataTable, based on Textual's DataTable."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Union

from rich.style import Style
from rich.text import Text
from textual import on, events
from textual.app import RenderResult
from textual.binding import Binding
from textual.color import Color
from textual.coordinate import Coordinate
from textual.filter import DimFilter
from textual.message import Message
from textual.message_pump import MessagePump
from textual.strip import Strip
from textual.widgets import DataTable
from textual.widgets.data_table import CellDoesNotExist, CellKey, RowKey

# typing_extensions for Self (Python 3.9 compat)
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class SpeekDataTable(DataTable[Union[str, Text]]):
    DEFAULT_CSS = """\
SpeekDataTable {
    &.empty {
        display: none;
    }
}
"""

    BINDINGS = [
        Binding("up,k", "cursor_up", "Cursor Up", show=False),
        Binding("down,j", "cursor_down", "Cursor Down", show=False),
        Binding("right", "cursor_right", "Cursor Right", show=False),
        Binding("left,h", "cursor_left", "Cursor Left", show=False),
        Binding("home", "scroll_home", "Home", show=False),
        Binding("end", "scroll_end", "End", show=False),
        Binding("g,ctrl+home", "scroll_top", "Top", show=False),
        Binding("G,ctrl+end", "scroll_bottom", "Bottom", show=False),
        Binding("s", "sort_column", "Sort", show=False),
        Binding("slash", "start_filter", "/ Filter", show=False),
    ]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.cursor_vertical_escape = True
        self.row_disable = False
        self.cursor_foreground_priority = "renderable"
        self.click_chain = None
        self._sort_col: int = -1
        self._sort_reverse: bool = False
        self._filter_text: str = ''
        self._filter_active: bool = False
        self._pre_filter_data: list | None = None  # stashed rows before filtering

    @dataclass
    class Checkbox:
        """A checkbox, added to rows to make them enable/disable."""

        data_table: "SpeekDataTable"
        checked: bool = True
        text: Text = field(default_factory=lambda: Text("✔︎"))

        def __rich__(self) -> RenderResult:
            return self.text

        def toggle(self) -> bool:
            """Toggle the checkbox."""
            self.checked = not self.checked
            self.text = Text("✔︎" if self.checked else " ")
            return self.checked

        @property
        def plain(self) -> str:
            return self.text.plain

    @dataclass
    class RowsRemoved(Message):
        data_table: "SpeekDataTable"
        explicit_by_user: bool = True

        @property
        def control(self) -> "SpeekDataTable":
            return self.data_table

    @dataclass
    class RowsAdded(Message):
        data_table: "SpeekDataTable"
        explicit_by_user: bool = True

        @property
        def control(self) -> "SpeekDataTable":
            return self.data_table

    def add_row(
        self,
        *cells: str | Text,
        height: int | None = 1,
        key: str | None = None,
        label: str | Text | None = None,
        explicit_by_user: bool = True,
        sender: MessagePump | None = None,
    ) -> RowKey:
        msg = self.RowsAdded(self, explicit_by_user=explicit_by_user)
        if sender:
            msg.set_sender(sender)
        self.post_message(msg)

        text_cells: list[Text] = []
        for cell in cells:
            if isinstance(cell, str):
                cell = Text(cell)
            text_cells.append(cell)

        if self.row_disable and label is None:
            label = self.Checkbox(self, True)

        return super().add_row(*text_cells, height=height, key=key, label=label)

    def action_toggle_fixed_columns(self) -> None:
        self.fixed_columns = 1 if self.fixed_columns == 0 else 0

    def remove_row(self, row_key: RowKey | str) -> None:
        self.post_message(self.RowsRemoved(self))
        rv = super().remove_row(row_key)
        self.column_width_refresh()
        return rv

    def clear(self, columns: bool = False) -> Self:
        self.post_message(self.RowsRemoved(self, explicit_by_user=False))
        super().clear(columns=columns)
        self.column_width_refresh()
        return self

    def replace_all_rows(
        self, rows: Iterable[Iterable[str]], enable_states: Iterable[bool] | None = None
    ) -> None:
        self.clear()
        if self.row_disable and enable_states:
            for row, enable in zip(rows, enable_states):
                self.add_row(
                    *row,
                    explicit_by_user=False,
                    label=self.Checkbox(self, enable),
                )
        else:
            for row in rows:
                self.add_row(*row, explicit_by_user=False)
        self.column_width_refresh()

    def column_width_refresh(self) -> None:
        if self.row_count > 0:
            row_zero = list(self._data.keys())[0]
            columns = set(list(self._data.values())[0].keys())
            self._update_column_widths(
                {CellKey(row_zero, column) for column in columns}
            )

    def action_cursor_down(self) -> None:
        self._set_hover_cursor(False)
        if (
            self.cursor_coordinate.row == self.row_count - 1
            and self.cursor_vertical_escape
        ):
            self.screen.focus_next()
        else:
            cursor_type = self.cursor_type
            if self.show_cursor and (cursor_type == "cell" or cursor_type == "row"):
                row, column = self.cursor_coordinate
                if row == self.row_count - 1:
                    self.cursor_coordinate = Coordinate(0, column)
                else:
                    self.cursor_coordinate = self.cursor_coordinate.down()
            else:
                super().action_cursor_down()

    def action_cursor_up(self) -> None:
        self._set_hover_cursor(False)
        if self.cursor_coordinate.row == 0 and self.cursor_vertical_escape:
            self.screen.focus_previous()
        else:
            cursor_type = self.cursor_type
            if self.show_cursor and (cursor_type == "cell" or cursor_type == "row"):
                row, column = self.cursor_coordinate
                if row == 0:
                    self.cursor_coordinate = Coordinate(self.row_count - 1, column)
                else:
                    self.cursor_coordinate = self.cursor_coordinate.up()
            else:
                super().action_cursor_up()

    @on(RowsRemoved)
    @on(RowsAdded)
    def _on_rows_removed(self, event: "SpeekDataTable.RowsRemoved | SpeekDataTable.RowsAdded") -> None:
        self.set_class(self.row_count == 0, "empty")

    def action_remove_row(self) -> None:
        try:
            cursor_cell_key = self.coordinate_to_cell_key(self.cursor_coordinate)
            cursor_row_key, _ = cursor_cell_key
            self.remove_row(cursor_row_key)
        except CellDoesNotExist:
            pass

    def action_toggle_row(self) -> None:
        try:
            cursor_cell_key = self.coordinate_to_cell_key(self.cursor_coordinate)
            cursor_row_key, _ = cursor_cell_key
            self.toggle_row(cursor_row_key)
        except CellDoesNotExist:
            pass

    def toggle_row(self, row_key: RowKey) -> None:
        try:
            checkbox: SpeekDataTable.Checkbox = self.rows[row_key].label
        except KeyError:
            return
        else:
            checkbox.toggle()
            self._update_count += 1
            self.refresh()

    def is_row_enabled_at(self, row_index: int) -> bool:
        row_key = self._row_locations.get_key(row_index)
        try:
            checkbox: SpeekDataTable.Checkbox = self.rows[row_key].label
        except KeyError:
            return True
        else:
            return checkbox.checked

    def render_line(self, y: int) -> Strip:
        strip = super().render_line(y)
        try:
            row_key, _ = self._get_offsets(y)
        except LookupError:
            return Strip.blank(self.size.width)

        try:
            label = self.rows[row_key].label
        except KeyError:
            return strip

        if label is None:
            return strip

        is_disabled = not label.checked
        if self.row_disable and is_disabled:
            strip = strip.apply_style(Style(dim=True))
            strip = strip.apply_filter(DimFilter(), Color(0, 0, 0))

        return strip

    @on(DataTable.RowLabelSelected)
    def _on_row_label_selected(self, event: DataTable.RowLabelSelected) -> None:
        if self.row_disable:
            event.prevent_default()
            self.toggle_row(event.row_key)

    async def _on_click(self, event: events.Click) -> None:
        self.click_chain = event.chain
        await super()._on_click(event)
        self.click_chain = None

    def post_message(self, message: Message) -> bool:
        if self.click_chain and isinstance(message, DataTable.RowSelected):
            message._click_chain = self.click_chain
        return super().post_message(message)

    # ── Sort ────────────────────────────────────────────────────────────────

    def _do_sort(self, col_idx: int) -> None:
        """Sort by column index, toggling direction."""
        if col_idx == self._sort_col:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_col = col_idx
            self._sort_reverse = False

        col_keys = list(self.columns.keys())
        if col_idx >= len(col_keys):
            return
        col_key = col_keys[col_idx]

        # Textual's sort passes the single cell value to key(), not the row tuple
        def _sort_key(cell_value) -> str:
            return cell_value.plain if hasattr(cell_value, 'plain') else str(cell_value)

        self.sort(col_key, key=_sort_key, reverse=self._sort_reverse)

    def action_sort_column(self) -> None:
        """Sort by the column under the cursor."""
        self._do_sort(self.cursor_coordinate.column)

    @on(DataTable.HeaderSelected)
    def _on_header_click(self, event: DataTable.HeaderSelected) -> None:
        """Sort by clicked column header."""
        self._do_sort(event.column_index)

    # ── Filter ─────────────────────────────────────────────────────────────

    def action_start_filter(self) -> None:
        """Enter filter mode — show input for text filtering."""
        if self._filter_active:
            self._end_filter()
            return
        self._filter_active = True
        # Stash current rows
        self._stash_rows()
        # Show filter via app notification prompt
        self.app.notify('Type to filter, Esc to clear', title='Filter', timeout=3)
        # We use on_key to capture filter text since there's no inline input in DataTable
        self._filter_text = ''

    def _stash_rows(self) -> None:
        """Save all current row data for filtering."""
        self._pre_filter_data = []
        for row_key in self._row_locations:
            try:
                cells = self.get_row(row_key)
                self._pre_filter_data.append((row_key, cells))
            except Exception:
                pass

    def _apply_filter(self) -> None:
        """Re-render table showing only rows matching filter text."""
        if not self._pre_filter_data:
            return
        query = self._filter_text.lower()
        with self.app.batch_update():
            self.clear()
            for row_key, cells in self._pre_filter_data:
                if not query:
                    self.add_row(*cells, key=str(row_key.value), explicit_by_user=False)
                    continue
                # Check if any cell contains the filter text
                match = any(
                    query in (c.plain.lower() if hasattr(c, 'plain') else str(c).lower())
                    for c in cells
                )
                if match:
                    self.add_row(*cells, key=str(row_key.value), explicit_by_user=False)

    def _end_filter(self) -> None:
        """Exit filter mode and restore all rows."""
        self._filter_active = False
        self._filter_text = ''
        if self._pre_filter_data:
            with self.app.batch_update():
                self.clear()
                for row_key, cells in self._pre_filter_data:
                    self.add_row(*cells, key=str(row_key.value), explicit_by_user=False)
        self._pre_filter_data = None

    def on_key(self, event: events.Key) -> None:
        """Handle filter input when filter mode is active."""
        if not self._filter_active:
            return
        if event.key == 'escape':
            event.prevent_default()
            event.stop()
            self._end_filter()
        elif event.key == 'backspace':
            event.prevent_default()
            event.stop()
            self._filter_text = self._filter_text[:-1]
            self._apply_filter()
            self.border_subtitle = f'/{self._filter_text}' if self._filter_text else ''
        elif event.is_printable and event.character:
            event.prevent_default()
            event.stop()
            self._filter_text += event.character
            self._apply_filter()
            self.border_subtitle = f'/{self._filter_text}'

    def __rich_repr__(self):
        yield "id", self.id
        yield "classes", self.classes
        yield "row_count", self.row_count
