"""modal_base.py — Shared base for all speek-max modal screens."""
from __future__ import annotations

from typing import Generic, TypeVar

from textual.screen import ModalScreen

_DIAG = '╱'
_R = TypeVar('_R')


class SpeekModal(ModalScreen[_R], Generic[_R]):
    """ModalScreen with a diagonal-hatch backdrop and drop-shadow popup support.

    Subclasses inherit the diagonal pattern automatically (rendered behind all
    children as the screen's own text layer).  Popup panels that use the CSS
    class ``speek-popup`` gain asymmetric borders that simulate a drop shadow.
    """

    DEFAULT_CSS = """
    SpeekModal {
        align: center middle;
        background: black 5%;
        color: #505050;
    }
    """

    # ── backdrop ────────────────────────────────────────────────────────────

    def render(self) -> str:
        """Fill every cell with ╱ at very low opacity.

        Fills every character cell so lines are as tight as terminal allows.
        The 5% black background keeps the app almost fully visible behind the
        characters; #505050 grey makes the diagonal grain subtly perceptible.
        """
        w = self.size.width
        h = self.size.height
        if not w or not h:
            return ''
        row = _DIAG * w
        return '\n'.join([row] * h)
