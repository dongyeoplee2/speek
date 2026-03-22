"""A modal screen for confirming a destructive action."""
from __future__ import annotations

from typing import Literal
from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Static

from speek.speek_max.widgets.modal_base import SpeekModal


class ConfirmationModal(SpeekModal[bool]):
    DEFAULT_CSS = """
    ConfirmationModal {
        align: center middle;
        width: 100%;
        height: 100%;

        & #confirmation-screen {
            width: 60;
            min-width: 40;
            max-width: 80%;
            height: auto;
            content-align: center middle;
        }

        & #confirmation-screen Static {
            width: 100%;
            text-align: center;
            color: $text;
            text-style: bold;
            margin-bottom: 1;
        }

        & #confirmation-buttons {
            margin-top: 1;
            width: 100%;
            height: 3;
            align: center middle;

            & > Button {
                width: 1fr;
                min-width: 12;
            }
        }
    }
    """

    BINDINGS = [
        Binding(
            "left,right,up,down,h,j,k,l",
            "move_focus",
            "Navigate",
            show=False,
        )
    ]

    def __init__(
        self,
        message: str,
        confirm_text: str = "Yes \\[y]",
        confirm_binding: str = "y",
        cancel_text: str = "No \\[n]",
        cancel_binding: str = "n",
        auto_focus: Literal["confirm", "cancel"] | None = "confirm",
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self.message = message
        self.confirm_text = confirm_text
        self.confirm_binding = confirm_binding
        self.cancel_text = cancel_text
        self.cancel_binding = cancel_binding
        self.auto_focus = auto_focus

    def on_mount(self) -> None:
        self._bindings.bind(self.confirm_binding, "screen.dismiss(True)")
        self._bindings.bind(self.cancel_binding, "screen.dismiss(False)")
        self._bindings.bind("escape", "screen.dismiss(False)")
        if self.auto_focus is not None:
            self.query_one(f"#{self.auto_focus}-button").focus()

    def compose(self) -> ComposeResult:
        """Compose the confirmation dialog."""
        with Vertical(id="confirmation-screen", classes="modal-body speek-popup") as container:
            container.border_title = "Confirm"
            yield Static(self.message)
            with Horizontal(id="confirmation-buttons"):
                yield Button(self.confirm_text, id="confirm-button")
                yield Button(self.cancel_text, id="cancel-button")

    @on(Button.Pressed, "#confirm-button")
    def confirm(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#cancel-button")
    def cancel(self) -> None:
        self.dismiss(False)

    def action_move_focus(self) -> None:
        self.screen.focus_next()
