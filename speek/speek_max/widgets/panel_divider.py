"""panel_divider.py — Draggable vertical divider for resizing left/right panels."""
from __future__ import annotations

from textual.widget import Widget
from textual.events import MouseDown, MouseMove, MouseUp


class PanelDivider(Widget):
    """Drag to resize #left-panel / #side-panel."""

    DEFAULT_CSS = """
    PanelDivider {
        width: 1;
        height: 1fr;
        background: $accent 10%;
        color: $accent 30%;
        &:hover {
            background: $accent 35%;
            color: $accent;
        }
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self._dragging = False
        self._start_x = 0
        self._left_start = 0

    def render(self) -> str:
        """Render the divider character."""
        return ("┃\n" * self.size.height).rstrip()

    def on_mouse_down(self, event: MouseDown) -> None:
        self._dragging = True
        self._start_x = event.screen_x
        self._left_start = self.app.query_one('#left-panel').size.width
        self.capture_mouse()
        event.stop()

    def on_mouse_move(self, event: MouseMove) -> None:
        if not self._dragging:
            return
        total = self.app.query_one('#main-layout').size.width
        dx = event.screen_x - self._start_x
        new_left = max(30, min(total - 16, self._left_start + dx))
        new_right = total - new_left - 1  # 1 for divider itself
        self.app.query_one('#left-panel').styles.width = new_left
        self.app.query_one('#side-panel').styles.width = new_right
        event.stop()

    def on_mouse_up(self, event: MouseUp) -> None:
        self._dragging = False
        self.release_mouse()
        event.stop()
