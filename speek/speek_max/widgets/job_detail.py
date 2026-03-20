"""job_detail.py — scontrol-based job detail modal."""
from __future__ import annotations

from typing import Dict

from rich.table import Table
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.widgets import Static

from speek.speek_max.widgets.modal_base import SpeekModal

from speek.speek_max._utils import tc


# Fields to display, in order: (scontrol key, display label)
_FIELDS = [
    # ── Identity ──────────────────────────────────────────────────────────────
    ('JobId',           'Job ID'),
    ('JobName',         'Name'),
    ('UserId',          'User'),
    ('GroupId',         'Group'),
    ('Account',         'Account'),
    ('QOS',             'QOS'),
    ('Priority',        'Priority'),
    ('Partition',       'Partition'),
    # ── Status ────────────────────────────────────────────────────────────────
    ('JobState',        'State'),
    ('ExitCode',        'Exit code'),
    ('DerivedExitCode', 'Derived exit'),
    ('FailedNode',      'Failed node'),
    ('Reason',          'Reason'),
    # ── Allocation ────────────────────────────────────────────────────────────
    ('NumNodes',        'Nodes'),
    ('NumCPUs',         'CPUs'),
    ('NTasks',          'Tasks'),
    ('NumTasks',        'Tasks'),    # scontrol key
    ('TRES',            'Alloc TRES'),
    ('ReqTRES',         'Req TRES'),
    ('ReqMem',          'Req mem'),
    ('NodeList',        'Node list'),
    # ── Timing ────────────────────────────────────────────────────────────────
    ('TimeLimit',       'Time limit'),
    ('SubmitTime',      'Submitted'),
    ('StartTime',       'Started'),
    ('EndTime',         'Ended'),
    ('Elapsed',         'Elapsed'),
    # ── Resource usage (from .batch step via sacct) ───────────────────────────
    ('MaxRSS',          'Peak RSS'),
    ('MaxVMSize',       'Peak VM'),
    ('AveRSS',          'Avg RSS'),
    ('MaxDiskRead',     'Disk read'),
    ('MaxDiskWrite',    'Disk write'),
    ('CPUTime',         'CPU alloc·t'),
    ('TotalCPU',        'CPU used'),
    ('UserCPU',         'CPU user'),
    ('SystemCPU',       'CPU sys'),
    # ── Paths ─────────────────────────────────────────────────────────────────
    ('WorkDir',         'Work dir'),
    ('Command',         'Command'),
    ('StdOut',          'Stdout'),
    ('StdErr',          'Stderr'),
    # ── Misc ──────────────────────────────────────────────────────────────────
    ('Comment',         'Comment'),
]


_STATE_COLORS = {
    'RUNNING':       'text-success',
    'PENDING':       'text-warning',
    'FAILED':        'text-error',
    'TIMEOUT':       'text-error',
    'CANCELLED':     'text-error',
    'COMPLETED':     'text-muted',
    'OUT_OF_MEMORY': 'text-error',
}


def _build_table(details: Dict[str, str], tv: Dict[str, str]) -> Table:
    c_label  = tc(tv, 'text-muted',    'bright_black')
    c_value  = tc(tv, 'text',          'default')
    c_accent = tc(tv, 'primary',       'magenta')
    c_muted  = tc(tv, 'text-muted',    'bright_black')

    table = Table(box=None, padding=(0, 1), show_header=False, expand=True)
    table.add_column('label', style=c_label,  width=14, no_wrap=True)
    table.add_column('value', style=c_value,  ratio=1,  no_wrap=False)

    if not details:
        table.add_row('', Text('No data available (scontrol + sacct returned nothing)', style=c_muted))
        return table

    state = details.get('JobState', '')
    state_color = tc(tv, _STATE_COLORS.get(state, 'text'), 'default')

    _seen_labels: set = set()
    for key, label in _FIELDS:
        val = details.get(key, '')
        if not val or val in ('(null)', 'None', 'N/A', 'Unknown'):
            continue
        # NTasks (sacct) and NumTasks (scontrol) share the label — show only first
        if label in _seen_labels:
            continue
        _seen_labels.add(label)
        if key == 'JobState':
            text = Text(val, style=f'bold {state_color}')
        elif key in ('JobId', 'JobName'):
            text = Text(val, style=f'bold {c_accent}')
        elif key in ('WorkDir', 'Command', 'StdOut', 'StdErr', 'TRES', 'ReqTRES', 'AllocTRES'):
            text = Text(val, style=c_muted, overflow='fold')
        elif key == 'FailedNode':
            text = Text(val, style=f'bold {tc(tv, "text-error", "red")}')
        elif key in ('ExitCode', 'DerivedExitCode') and val not in ('0:0', '0'):
            text = Text(val, style=tc(tv, 'text-error', '#FF4500'))
        else:
            text = Text(val)
        table.add_row(label, text)

    return table


class JobDetailModal(SpeekModal):
    DEFAULT_CSS = """
    JobDetailModal {
        align: center middle;
    }
    #job-detail-body {
        width: 72;
        height: auto;
        max-height: 80%;
        background: $background;
        border: wide $accent;
        border-title-color: $primary;
        border-title-background: $background;
        border-title-style: bold;
        padding: 1 2;
        overflow-y: auto;
    }
    """

    BINDINGS = [
        Binding('escape,q,d', 'dismiss', 'Close', show=True),
    ]

    def __init__(self, job_id: str, details: Dict[str, str]) -> None:
        super().__init__()
        self.job_id = job_id
        self.details = details

    def compose(self) -> ComposeResult:
        """Compose the job detail panel."""
        body = Static(id='job-detail-body', classes='speek-popup')
        body.border_title = f'Job {self.job_id}'
        yield body

    def on_mount(self) -> None:
        tv = self.app.theme_variables
        table = _build_table(self.details, tv)
        self.query_one('#job-detail-body', Static).update(table)
