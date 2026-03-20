"""submit_widget.py — Enhanced sbatch submission: preset scripts/names, multi-config, quick-drop."""
from __future__ import annotations

import json
import os
import re
import subprocess
from itertools import count as _itercount
from typing import List, Optional, Tuple

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, HorizontalScroll, Vertical
from textual.message import Message
from textual.widget import Widget

from speek.speek_max.widgets.modal_base import SpeekModal
from textual.widgets import Button, Input, Label, Select, Static

from speek.speek_max.slurm import get_partitions
from speek.speek_max.widgets.input import SpeekInput
from speek.speek_max.widgets.select import SpeekSelect

_STORE_PATH  = os.path.expanduser('~/.config/speek/submit_store.json')
_ADD_NEW     = '__add_new__'
_SEL_SCRIPT  = '#sw-script'
_SEL_NAME    = '#sw-name'
_SEL_QSCRIPT = '#sw-q-script'
_cfg_seq     = _itercount(1)
_NO_PARTS    = [('(none)', '')]


# ── Persistence ────────────────────────────────────────────────────────────────

def _load_store() -> dict:
    try:
        with open(_STORE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_store(d: dict) -> None:
    os.makedirs(os.path.dirname(_STORE_PATH), exist_ok=True)
    with open(_STORE_PATH, 'w') as f:
        json.dump(d, f, indent=2)


def _load_list(key: str) -> List[str]:
    return _load_store().get(key, [])


def _save_item(key: str, value: str) -> None:
    store = _load_store()
    lst   = [x for x in store.get(key, []) if x != value]
    lst.insert(0, value)
    store[key] = lst[:30]
    _save_store(store)


# ── Add-item modal ─────────────────────────────────────────────────────────────

class _AddModal(SpeekModal):
    """Tiny modal: type a new script path or job name."""

    BINDINGS = [Binding('escape', 'dismiss_none', '', show=False)]

    DEFAULT_CSS = """
    _AddModal { align: center middle; }
    #am-body {
        width: 64; height: auto;
        background: $background;
        border: wide $accent;
        border-title-color: $background;
        border-title-background: $accent;
        border-title-style: bold;
        padding: 1 2;
    }
    #am-hint { height: 1; color: $text-muted; margin-top: 1; }
    """

    def __init__(self, modal_title: str, placeholder: str = '') -> None:
        super().__init__()
        self._modal_title = modal_title
        self._placeholder = placeholder

    def compose(self) -> ComposeResult:
        with Vertical(id='am-body', classes='speek-popup') as v:
            v.border_title = self._modal_title
            yield Input(placeholder=self._placeholder, id='am-input')
            yield Static('[bold]Enter[/] confirm  [bold]Esc[/] cancel',
                         id='am-hint', markup=True)

    def on_mount(self) -> None:
        self.query_one('#am-input', Input).focus()

    def on_input_submitted(self, _: Input.Submitted) -> None:
        self.dismiss(self.query_one('#am-input', Input).value.strip() or None)

    def action_dismiss_none(self) -> None:
        self.dismiss(None)


# ── Config row ─────────────────────────────────────────────────────────────────

class ConfigRow(Widget):
    """One (partition × GPU count × job count) configuration block."""

    class Removed(Message):
        def __init__(self, row: 'ConfigRow') -> None:
            super().__init__()
            self.row = row

    DEFAULT_CSS = """
    ConfigRow {
        layout: horizontal;
        width: auto;
        height: 1;
        margin-right: 2;
    }
    ConfigRow Button.cfg-del {
        min-width: 3; width: 3; height: 1;
        border: none; padding: 0 1;
        background: $error-muted; color: $text-error;
    }
    ConfigRow SpeekSelect { width: 14; height: 1; }
    ConfigRow SpeekInput  { width: 5;  height: 1; }
    ConfigRow Label.cfg-sep {
        width: auto; height: 1; padding: 0 1; color: $text-muted;
    }
    """

    def __init__(self, row_id: str, partitions: List[str]) -> None:
        super().__init__(id=row_id)
        self._partitions = partitions

    def compose(self) -> ComposeResult:
        opts = [(p, p) for p in self._partitions] or _NO_PARTS
        yield Button('×', id=f'{self.id}-del', classes='cfg-del')
        yield SpeekSelect(opts, id=f'{self.id}-part', allow_blank=False)
        yield Label('×', classes='cfg-sep')
        yield SpeekInput(placeholder='GPU',  id=f'{self.id}-gpu',  type='integer')
        yield Label('GPU ×', classes='cfg-sep')
        yield SpeekInput(placeholder='1',    id=f'{self.id}-jobs', type='integer')
        yield Label('jobs', classes='cfg-sep')

    def set_partitions(self, partitions: List[str]) -> None:
        self._partitions = partitions
        opts = [(p, p) for p in partitions] or _NO_PARTS
        try:
            self.query_one(f'#{self.id}-part', SpeekSelect).set_options(opts)
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == f'{self.id}-del':
            event.stop()
            self.post_message(ConfigRow.Removed(self))

    def values(self) -> Tuple[str, str, str]:
        """Return (partition, gpus, n_jobs)."""
        def _s(widget_id: str) -> str:
            try:
                v = self.query_one(f'#{widget_id}', SpeekSelect).value
                return str(v) if v is not Select.BLANK else ''
            except Exception:
                return ''

        def _i(widget_id: str) -> str:
            try:
                return self.query_one(f'#{widget_id}', SpeekInput).value or ''
            except Exception:
                return ''

        return _s(f'{self.id}-part'), _i(f'{self.id}-gpu'), _i(f'{self.id}-jobs')


# ── Submit widget ──────────────────────────────────────────────────────────────

class SubmitWidget(Widget):
    """
    sbatch submission panel with:
    - Dropdown script-path & job-name with persistent history (+ add-new option)
    - Multiple (partition × GPU × jobs) configs, keyboard-addable/removable
    - Quick-drop row: N jobs × K GPU on partition
    """

    BORDER_TITLE = 'Submit'

    BINDINGS = [
        Binding('ctrl+s', 'submit_job', 'Submit',    show=True),
        Binding('ctrl+a', 'add_config', '+ Config',  show=True),
        Binding('ctrl+r', 'quick_run',  'Quick Run', show=True),
    ]

    class JobSubmitted(Message):
        def __init__(self, job_id: str) -> None:
            super().__init__()
            self.job_id = job_id

    class SubmitError(Message):
        def __init__(self, message: str) -> None:
            super().__init__()
            self.message = message

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._script_paths: List[str] = []
        self._job_names:    List[str] = []
        self._partitions:   List[str] = []

    # ── Compose ────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        self._script_paths = _load_list('script_paths')
        self._job_names    = _load_list('job_names')
        # _partitions loaded async in on_mount — don't block compose with sinfo

        sp_opts = self._opts(self._script_paths, '+ Add script path…')
        nm_opts = self._opts(self._job_names,    '+ Add job name…')
        pt_opts = [('(loading…)', '')]

        with Horizontal(id='sw-header'):
            yield Label('Script', classes='submit-lbl')
            yield SpeekSelect(sp_opts, id='sw-script', allow_blank=True)
            yield Label('Name',   classes='submit-lbl')
            yield SpeekSelect(nm_opts, id='sw-name',   allow_blank=True)

        with HorizontalScroll(id='sw-configs'):
            yield ConfigRow(f'cfg-{next(_cfg_seq)}', self._partitions)

        with Horizontal(id='sw-actions'):
            yield Button('+ Config  Ctrl+A', id='sw-add-cfg')
            yield Button('Submit  Ctrl+S',   id='submit-btn', variant='success')

        with Horizontal(id='sw-quick'):
            yield Label('Quick', classes='submit-lbl')
            yield SpeekInput(placeholder='1', id='sw-q-jobs', type='integer')
            yield Label('jobs ×', classes='submit-lbl')
            yield SpeekInput(placeholder='4', id='sw-q-gpu',  type='integer')
            yield Label('GPU on', classes='submit-lbl')
            yield SpeekSelect(pt_opts, id='sw-q-part',   allow_blank=False)
            yield Label('script', classes='submit-lbl')
            yield SpeekSelect(sp_opts, id='sw-q-script', allow_blank=True)
            yield Button('▶ Run  Ctrl+R', id='sw-q-btn', variant='success')

    def on_mount(self) -> None:
        if getattr(self.app, '_cmd_sinfo', True):
            self.run_worker(self._load_partitions, thread=True, group='partitions')

    def _load_partitions(self) -> None:
        partitions = get_partitions()
        self.app.call_from_thread(self._apply_partitions, partitions)

    def _apply_partitions(self, partitions: List[str]) -> None:
        self._partitions = partitions
        pt_opts = [(p, p) for p in partitions] or _NO_PARTS
        for sel_id in ('#sw-q-part',):
            try:
                self.query_one(sel_id, SpeekSelect).set_options(pt_opts)
            except Exception:
                pass
        for row in self.query(ConfigRow):
            row.set_partitions(partitions)

    # ── Option helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _opts(items: List[str], add_label: str) -> List[Tuple[str, str]]:
        return [(p, p) for p in items] + [(add_label, _ADD_NEW)]

    def _refresh_script_opts(self) -> None:
        opts = self._opts(self._script_paths, '+ Add script path…')
        for sel_id in (_SEL_SCRIPT, _SEL_QSCRIPT):
            try:
                self.query_one(sel_id, SpeekSelect).set_options(opts)
            except Exception:
                pass

    def _refresh_name_opts(self) -> None:
        opts = self._opts(self._job_names, '+ Add job name…')
        try:
            self.query_one(_SEL_NAME, SpeekSelect).set_options(opts)
        except Exception:
            pass

    # ── "Add new" trigger ──────────────────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        if str(event.value) != _ADD_NEW:
            return
        event.stop()
        sid = event.select.id or ''
        if sid in ('sw-script', 'sw-q-script'):
            self._open_add_modal('script_paths', f'#{sid}',
                                 'Add script path', '/path/to/script.sh')
        elif sid == 'sw-name':
            self._open_add_modal('job_names', _SEL_NAME,
                                 'Add job name', 'my-job')

    def _open_add_modal(self, key: str, sel_id: str,
                        title: str, hint: str) -> None:
        def _cb(value: Optional[str]) -> None:
            if not value:
                return
            _save_item(key, value)
            if key == 'script_paths':
                self._script_paths = _load_list('script_paths')
                self._refresh_script_opts()
            else:
                self._job_names = _load_list('job_names')
                self._refresh_name_opts()
            try:
                self.query_one(sel_id, SpeekSelect).value = value
            except Exception:
                pass

        self.app.push_screen(_AddModal(title, hint), _cb)

    # ── Config management ──────────────────────────────────────────────────────

    def on_config_row_removed(self, event: ConfigRow.Removed) -> None:
        rows = list(self.query(ConfigRow))
        if len(rows) > 1:
            event.row.remove()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        handlers = {
            'sw-add-cfg': self.action_add_config,
            'submit-btn': self.action_submit_job,
            'sw-q-btn':   self.action_quick_run,
        }
        fn = handlers.get(event.button.id or '')
        if fn:
            fn()

    def action_add_config(self) -> None:
        row = ConfigRow(f'cfg-{next(_cfg_seq)}', self._partitions)
        self.query_one('#sw-configs').mount(row)
        self.set_timer(0.05, row.focus)

    # ── Value helpers ──────────────────────────────────────────────────────────

    def _sel(self, sel_id: str) -> str:
        try:
            v = self.query_one(sel_id, SpeekSelect).value
            s = str(v)
            return s if v is not Select.BLANK and s != _ADD_NEW else ''
        except Exception:
            return ''

    def _inp(self, inp_id: str) -> str:
        try:
            return self.query_one(inp_id, SpeekInput).value or ''
        except Exception:
            return ''

    # ── Submit actions ─────────────────────────────────────────────────────────

    def action_submit_job(self) -> None:
        script = self._sel(_SEL_SCRIPT)
        name   = self._sel(_SEL_NAME)
        if not script:
            self.app.notify('Select a script path first', severity='warning')
            return
        _save_item('script_paths', script)
        if name:
            _save_item('job_names', name)
        cmds: List[List[str]] = []
        for row in self.query(ConfigRow):
            part, gpus, n_str = row.values()
            n = max(1, int(n_str or 1))
            for _ in range(n):
                cmd = ['sbatch']
                if part:  cmd += ['--partition', part]
                if gpus:  cmd += ['--gres', f'gpu:{gpus}']
                if name:  cmd += ['--job-name', name]
                cmd.append(script)
                cmds.append(cmd)
        self._run(cmds)

    def action_quick_run(self) -> None:
        script = self._sel(_SEL_QSCRIPT)
        if not script:
            self.app.notify('Select a script path first', severity='warning')
            return
        n_jobs = max(1, int(self._inp('#sw-q-jobs') or 1))
        n_gpu  = self._inp('#sw-q-gpu')
        part   = self._sel('#sw-q-part')
        cmds   = []
        for _ in range(n_jobs):
            cmd = ['sbatch']
            if part:  cmd += ['--partition', part]
            if n_gpu: cmd += ['--gres', f'gpu:{n_gpu}']
            cmd.append(script)
            cmds.append(cmd)
        _save_item('script_paths', script)
        self._run(cmds)

    @staticmethod
    def _sbatch_one(cmd: List[str]) -> Tuple[Optional[str], Optional[str]]:
        """Run one sbatch command. Returns (job_id, error_msg)."""
        try:
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
            m = re.search(r'(\d+)', out)
            return (m.group(1) if m else None), None
        except subprocess.CalledProcessError as e:
            return None, (e.output or str(e)).strip()

    def _notify_results(self, job_ids: List[str], errors: List[str]) -> None:
        if job_ids:
            n, s = len(job_ids), ', '.join(job_ids)
            msg = f'{n} job{"s" if n > 1 else ""} submitted: {s}'
            self.app.notify(msg, severity='information')
            self.post_message(self.JobSubmitted(job_ids[-1]))
        for err in errors:
            self.app.notify(f'sbatch: {err}', severity='error', timeout=10)

    def _run(self, cmds: List[List[str]]) -> None:
        def _worker() -> None:
            job_ids: List[str] = []
            errors:  List[str] = []
            for cmd in cmds:
                jid, err = self._sbatch_one(cmd)
                if jid:
                    job_ids.append(jid)
                if err:
                    errors.append(err)
            self.app.call_from_thread(self._notify_results, job_ids, errors)

        self.run_worker(_worker, thread=True, group='sbatch')
