"""submit_widget.py — sbatch job submission form."""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
from typing import List

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Button, Label, Static, TextArea

from speek.speek_max.slurm import get_partitions
from speek.speek_max.widgets.input import SpeekInput
from speek.speek_max.widgets.select import SpeekSelect


def _build_script(
    partition: str, gpus: str, ntasks: str,
    cpus: str, time_limit: str, job_name: str,
) -> str:
    lines = ['#!/bin/bash']
    if partition:
        lines.append(f'#SBATCH --partition={partition}')
    if gpus:
        lines.append(f'#SBATCH --gres=gpu:{gpus}')
    if ntasks:
        lines.append(f'#SBATCH --ntasks-per-node={ntasks}')
    if cpus:
        lines.append(f'#SBATCH --cpus-per-task={cpus}')
    if time_limit:
        lines.append(f'#SBATCH --time={time_limit}')
    if job_name:
        lines.append(f'#SBATCH --job-name={job_name}')
    lines += ['', '# Your commands here', '']
    return '\n'.join(lines)


class SubmitWidget(Widget):
    """Interactive sbatch submission form."""

    BORDER_TITLE = "Submit"

    BINDINGS = [
        Binding('ctrl+s', 'submit_job', 'Submit', show=True),
    ]

    class JobSubmitted(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    class SubmitError(Message):
        def __init__(self, message: str) -> None:
            super().__init__()
            self.message = message

    def compose(self) -> ComposeResult:
        partitions = get_partitions()
        part_opts = [(p, p) for p in partitions] if partitions else [('(none)', '')]

        with Horizontal(id='submit-fields'):
            yield Label('Partition', classes='submit-lbl')
            yield SpeekSelect(part_opts, id='part-select', allow_blank=False)
            yield Label('GPUs', classes='submit-lbl')
            yield SpeekInput(placeholder='4', id='gpu-count', type='integer')
            yield Label('Time', classes='submit-lbl')
            yield SpeekInput(placeholder='24:00:00', id='time-limit')
            yield Label('Name', classes='submit-lbl')
            yield SpeekInput(placeholder='my-job', id='job-name')
            yield Label('×', classes='submit-lbl')
            yield SpeekInput(placeholder='1', id='repeat-count', type='integer')
            yield Button('Submit', id='submit-btn', variant='success')

        yield TextArea('', id='script-preview', language='bash')

    def on_mount(self) -> None:
        self._update_preview()

    def on_input_changed(self, event) -> None:
        self._update_preview()

    def on_select_changed(self, event) -> None:
        self._update_preview()

    def _field(self, wid: str) -> str:
        try:
            return str(self.query_one(wid, SpeekInput).value or '')
        except Exception:
            return ''

    def _part(self) -> str:
        try:
            from textual.widgets import Select
            v = self.query_one('#part-select', SpeekSelect).value
            return str(v) if v and v is not Select.BLANK else ''
        except Exception:
            return ''

    def _update_preview(self) -> None:
        script = _build_script(
            partition=self._part(),
            gpus=self._field('#gpu-count'),
            ntasks='',
            cpus='',
            time_limit=self._field('#time-limit'),
            job_name=self._field('#job-name'),
        )
        try:
            ta = self.query_one('#script-preview', TextArea)
            ta.load_text(script)
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'submit-btn':
            self.action_submit_job()

    def action_submit_job(self) -> None:
        try:
            ta = self.query_one('#script-preview', TextArea)
            script = ta.text
        except Exception:
            self.app.notify('No script to submit', severity='error')
            return

        try:
            repeat = max(1, int(self._field('#repeat-count') or '1'))
        except ValueError:
            repeat = 1

        def _submit() -> None:
            try:
                with tempfile.NamedTemporaryFile(
                    mode='w', suffix='.sh', delete=False
                ) as f:
                    f.write(script)
                    tmp = f.name
                os.chmod(tmp, 0o755)
                job_ids = []
                for _ in range(repeat):
                    out = subprocess.check_output(
                        ['sbatch', tmp], text=True, stderr=subprocess.STDOUT
                    )
                    m = re.search(r'(\d+)', out)
                    if m:
                        job_ids.append(m.group(1))
                os.unlink(tmp)
                summary = ', '.join(job_ids) if job_ids else '?'
                msg = f'{len(job_ids)} job(s) submitted: {summary}' if repeat > 1 else f'Job {summary} submitted'
                self.app.call_from_thread(
                    lambda: self.app.notify(msg, severity='information')
                )
                if job_ids:
                    jid = job_ids[-1]
                    self.app.call_from_thread(
                        lambda: self.post_message(self.JobSubmitted(jid))
                    )
            except subprocess.CalledProcessError as e:
                msg = e.output.strip()
                self.app.call_from_thread(
                    lambda: self.post_message(self.SubmitError(msg))
                )
                self.app.call_from_thread(
                    lambda: self.app.notify(f'sbatch failed: {msg}', severity='error', timeout=10)
                )
            except Exception as exc:
                err = str(exc)
                self.app.call_from_thread(
                    lambda: self.app.notify(err, severity='error')
                )

        self.run_worker(_submit, thread=True, group='sbatch')
