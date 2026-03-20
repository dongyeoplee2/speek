"""quick_submit.py — One-line sbatch submit bar."""
from __future__ import annotations

import subprocess

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widget import Widget
from textual.widgets import Button, Input, Label


class QuickSubmitBar(Widget):
    """One-line submit bar: sbatch <script> [args]."""

    BORDER_TITLE = "Submit"

    BINDINGS = [
        Binding('ctrl+s', 'submit', 'Submit', show=True),
    ]

    def compose(self) -> ComposeResult:
        """Compose the quick-submit form."""
        with Horizontal(id='quick-submit-inner'):
            yield Label('sbatch', id='quick-submit-label')
            yield Input(placeholder='script.sh --arg value …', id='quick-submit-input')
            yield Button('Run', id='quick-submit-btn', variant='success')

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'quick-submit-btn':
            self.action_submit()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.action_submit()

    def action_submit(self) -> None:
        inp = self.query_one('#quick-submit-input', Input)
        raw = inp.value.strip()
        if not raw:
            return
        cmd = ['sbatch'] + raw.split()

        def _run() -> None:
            try:
                out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
                self.app.call_from_thread(
                    self.app.notify, out.strip(), title='sbatch', severity='information'
                )
                self.app.call_from_thread(self._clear)
            except subprocess.CalledProcessError as e:
                msg = (e.output or str(e)).strip()
                self.app.call_from_thread(
                    self.app.notify, msg, title='sbatch failed', severity='error'
                )

        self.run_worker(_run, thread=True, group='quick-submit')

    def _clear(self) -> None:
        self.query_one('#quick-submit-input', Input).value = ''
