"""job_info_modal.py — Combined log-output + scontrol-detail modal with job navigation."""
from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

from rich.table import Table as _RichTable
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import Button, ContentSwitcher, RichLog, Static

from speek.speek_max.widgets.modal_base import SpeekModal

from speek.speek_max.widgets.job_detail import _build_table

_GPU_RE        = re.compile(r'gres/gpu(?::([a-z0-9_-]+))?[=:](\d+)', re.IGNORECASE)
_JI_LOG        = '#ji-log'
_DETAIL_PANE   = 'ji-detail-pane'
_OUTPUT_PANE   = 'ji-output-pane'
_GPU_PANE      = 'ji-gpu-pane'
_PRIORITY_PANE = 'ji-priority-pane'  # kept for compat
_ANALYSIS_PANE = 'ji-priority-pane'  # same pane, renamed conceptually

# Log highlighting — ordered least-specific → most-specific so later patterns win.
_LOG_HIGHLIGHTS = (
    (r'\b\d+\.?\d*(?:[eE][+-]?\d+)?\b',                                               'green'),        # numbers + sci notation
    (r'\b0[xX][0-9A-Fa-f]+\b',                                                        'bold green'),   # hex literals
    (r'\b\d+\.?\d*\s*[KMGTPEkmgtpe]i?[Bb](?:ytes?)?\b',                               'bold green'),   # memory sizes
    (r'\b\d+\.?\d*(?:ns|us|ms|s|min|h)\b',                                            'green'),        # durations
    (r'\b\d+(?:\.\d+)?%',                                                              'bold magenta'), # percentages
    (r'\b\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\b',                                            'cyan'),         # HH:MM:SS
    (r'\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)?\b',                 'bold cyan'),    # ISO datetime
    (r'\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b',                                         'cyan'),         # IP address
    (r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b', 'magenta'), # UUID
    (r'\b[A-Za-z_]\w*(?=\s*=)',                                                        'yellow'),       # key= names
    (r'"[^"\n]*"',                                                                     'green'),        # double-quoted strings
    (r'(?<!\w)/[\w.\-/]+',                                                             'blue'),         # file paths
    (r'https?://\S+',                                                                  'bold blue'),    # URLs
    (r'\b(?:true|True|TRUE|false|False|FALSE)\b',                                      'bold cyan'),    # booleans
    (r'\b(?:null|NULL|None|nil|undefined)\b',                                          'dim'),          # null-like
    (r'\b(?:DEBUG|TRACE|VERBOSE)\b',                                                   'dim'),          # debug levels
    (r'\b(?:INFO|NOTICE)\b',                                                           'blue'),         # info levels
    (r'\b(?:WARNING|WARN)\b',                                                          'bold yellow'),  # warnings
    (r'\b(?:ERROR|FATAL|CRITICAL)\b',                                                  'bold red'),     # errors
    (r'\b(?:SUCCESS|DONE|COMPLETED|PASSED|OK)\b',                                      'bold green'),   # success
)


def _highlight_log(content: Text) -> None:
    """Apply token-level highlighting to log text in-place."""
    for pattern, style in _LOG_HIGHLIGHTS:
        content.highlight_regex(pattern, style)


# Error patterns to extract from log tail — (regex, label)
_ERROR_PATTERNS = [
    # Python
    (r'(\w+Error: .+)',                         'Python error'),
    (r'(\w+Exception: .+)',                     'Python exception'),
    (r'(Traceback \(most recent call last\))',   'Traceback'),
    (r'(AssertionError.*)',                      'Assertion'),
    # CUDA / GPU
    (r'(CUDA out of memory\..*)',               'CUDA OOM'),
    (r'(CUDA error: .+)',                       'CUDA error'),
    (r'(RuntimeError: CUDA.+)',                 'CUDA runtime'),
    (r'(NCCL error.+)',                         'NCCL error'),
    (r'(cuDNN error.+)',                        'cuDNN error'),
    # System
    (r'(Segmentation fault.*)',                 'Segfault'),
    (r'(Killed)',                               'OOM killed'),
    (r'(Bus error.*)',                          'Bus error'),
    (r'(Permission denied.*)',                  'Permission'),
    (r'(No such file or directory.*)',          'File not found'),
    (r'(Disk quota exceeded.*)',               'Disk quota'),
    # SLURM
    (r'(slurmstepd: error:.+)',                'SLURM step error'),
    (r'(DUE TO TIME LIMIT)',                   'Time limit'),
    (r'(oom-kill:.+)',                         'OOM kill'),
    # General
    (r'(ImportError: .+)',                      'Import error'),
    (r'(ModuleNotFoundError: .+)',             'Module not found'),
    (r'(FileNotFoundError: .+)',               'File not found'),
    (r'(KeyError: .+)',                        'Key error'),
    (r'(ValueError: .+)',                      'Value error'),
    (r'(TypeError: .+)',                       'Type error'),
]


def _extract_errors(log_text: str, tail_lines: int = 50) -> List[tuple[str, str]]:
    """Extract error messages from the last N lines of log output.

    Uses both regex patterns and fuzzy heuristics to catch unexpected errors.
    Returns list of (label, matched_text) tuples.
    """
    import re
    lines = log_text.splitlines()[-tail_lines:]
    tail = '\n'.join(lines)
    found: List[tuple[str, str]] = []
    seen: set[str] = set()

    # 1. Known patterns
    for pattern, label in _ERROR_PATTERNS:
        for m in re.finditer(pattern, tail):
            text = m.group(1).strip()[:200]
            if text not in seen:
                seen.add(text)
                found.append((label, text))

    # 2. Fuzzy heuristics — catch lines that look like errors but don't match known patterns
    _ERROR_SIGNALS = re.compile(
        r'(?i)\b(error|exception|fatal|critical|abort|panic|fail|denied|refused'
        r'|cannot|could not|unable to|not found|no such|invalid|illegal'
        r'|segfault|killed|oom|out of memory|exceeded|overflow|corrupt)\b'
    )
    _NOISE = re.compile(
        r'(?i)^(\s*$|#|//|--|\d+[/%]|.*\blog\.?(ging)?\.?(info|debug|warn)|.*progress|.*eta\b|.*epoch)'
    )
    for line in lines:
        stripped = line.strip()
        if not stripped or len(stripped) < 10 or len(stripped) > 300:
            continue
        if stripped in seen or _NOISE.match(stripped):
            continue
        if _ERROR_SIGNALS.search(stripped):
            # Classify by signal word
            lower = stripped.lower()
            if 'memory' in lower or 'oom' in lower:
                label = 'Memory'
            elif 'permission' in lower or 'denied' in lower:
                label = 'Permission'
            elif 'not found' in lower or 'no such' in lower:
                label = 'Not found'
            elif 'timeout' in lower or 'exceeded' in lower:
                label = 'Limit exceeded'
            else:
                label = 'Error'
            seen.add(stripped)
            found.append((label, stripped[:200]))

    return found


def _build_gpu_table(result: Dict) -> _RichTable:
    """Render fetch_job_gpu_stats result as a Rich table."""
    t = _RichTable(box=None, show_header=True, expand=True,
                   padding=(0, 1), show_edge=False)
    t.add_column('Host',    style='dim',   no_wrap=True)
    t.add_column('IDX',     justify='right')
    t.add_column('GPU',     style='bold',  min_width=12, no_wrap=True)
    t.add_column('GPU%',    justify='right', min_width=5)
    t.add_column('MEM%',    justify='right', min_width=5)
    t.add_column('Used',    justify='right', style='cyan')
    t.add_column('Total',   justify='right', style='dim')
    t.add_column('°C',      justify='right')
    t.add_column('W',       justify='right')

    rows = result.get('gpu_rows', [])
    if not rows:
        t.add_row('(no GPU data)', '', '', '', '', '', '', '', '')
        return t

    for r in rows:
        host, idx, name, gpu_p, mem_p, mem_used, mem_tot, temp, power = r[:9]
        try:
            gv = float(gpu_p)
            gs = 'bold red' if gv > 80 else ('bold yellow' if gv > 50 else 'bold green')
            gc = f'[{gs}]{gpu_p}%[/{gs}]'
        except ValueError:
            gc = f'{gpu_p}%'
        try:
            mv = float(mem_p)
            ms = 'bold red' if mv > 80 else ('bold yellow' if mv > 50 else 'bold cyan')
            mc = f'[{ms}]{mem_p}%[/{ms}]'
        except ValueError:
            mc = f'{mem_p}%'
        t.add_row(host, idx, name[:18], gc, mc,
                  f'{mem_used} MiB', f'{mem_tot} MiB', f'{temp}°C', f'{power} W')
    return t


def _title_from_details(job_id: str, details: Dict[str, str]) -> str:
    parts = [f'Job {job_id}']
    name = details.get('JobName', '')
    if name and name != 'batch':
        parts.append(name)
    m = _GPU_RE.search(details.get('AllocTRES', '') or details.get('TRES', ''))
    if m:
        model = m.group(1) or ''
        count = m.group(2) or '?'
        parts.append(f'{model or "GPU"}×{count}')
    node = details.get('NodeList', '') or details.get('BatchHost', '')
    if node and node not in ('None', 'none', ''):
        parts.append(node)
    part = details.get('Partition', '')
    if part:
        parts.append(part)
    return '  '.join(parts)


class JobInfoModal(SpeekModal):
    """Two-pane modal: scontrol detail (1) and job stdout (2), with job navigation."""

    BINDINGS = [
        Binding('escape,q', 'dismiss',        'Close',    show=True),
        Binding('tab',      'switch_tab',     '⇥ Tab',    show=True),
        Binding('1',        'show_detail',    'Detail',   show=True),
        Binding('2',        'show_output',    'Output',   show=True),
        Binding('3',        'show_gpu',       'GPU',      show=True),
        Binding('g',        'fetch_gpu',      '⚡ Fetch',  show=True),
        Binding('r',        'refresh',        'Refresh',  show=True),
        Binding('l,right',  'next_job',       '→ Next',   show=True),
        Binding('h,left',   'prev_job',       '← Prev',   show=True),
        Binding('j,down',   'scroll_down',    '',         show=False),
        Binding('k,up',     'scroll_up',      '',         show=False),
        Binding('ctrl+d',   'page_down',      '',         show=False),
        Binding('ctrl+u',   'page_up',        '',         show=False),
    ]

    DEFAULT_CSS = """
    JobInfoModal {
        align: center middle;
    }
    #ji-body {
        width: 90%;
        height: 85%;
        background: $background;
        border: wide $accent;
        border-title-color: $background;
        border-title-background: $accent;
        border-title-style: bold;
        padding: 0;
    }
    #ji-main {
        height: 1fr;
    }
    #ji-switcher {
        width: 1fr;
        height: 1fr;
    }
    #ji-detail-pane {
        height: 1fr;
        background: transparent;
    }
    #ji-detail-scroll {
        width: 1fr;
        height: 1fr;
        padding: 1 2;
    }
    #ji-analysis-scroll {
        width: 1fr;
        height: 1fr;
        padding: 1 2;
        border-left: tall $panel;
    }
    #ji-detail {
        height: auto;
        width: 1fr;
    }
    #ji-output-pane {
        height: 1fr;
        background: transparent;
    }
    #ji-log-path {
        height: 1;
        padding: 0 1;
        color: $text-muted;
        background: $surface;
        text-style: italic;
    }
    #ji-log {
        height: 1fr;
        background: transparent;
        padding: 0 1;
    }
    #ji-tab-sidebar {
        width: 10;
        height: 1fr;
        border-left: tall $panel;
        background: $panel;
        padding: 1 0;
        align-horizontal: center;
    }
    .ji-tab-btn {
        height: 4;
        width: 1fr;
        padding: 1;
        text-align: center;
        color: $text-muted;
    }
    .ji-tab-btn.--active {
        color: $primary;
        text-style: bold;
        background: $surface;
        border-left: wide $accent;
    }
    #ji-gpu-pane {
        height: 1fr;
        background: transparent;
    }
    #ji-gpu-toolbar {
        height: 1;
        padding: 0 1;
        background: $surface;
    }
    #ji-gpu-fetch-btn {
        height: 1;
        border: none;
        padding: 0 1;
        background: $accent-muted;
        color: $text-accent;
        min-width: 14;
    }
    #ji-gpu-status {
        height: 1;
        width: 1fr;
        padding: 0 1;
        color: $text-muted;
    }
    #ji-gpu-scroll {
        height: 1fr;
        padding: 0 1;
        background: transparent;
    }
    #ji-priority-content {
        height: auto;
        width: 1fr;
    }
    #ji-hint {
        height: 1;
        padding: 0 1;
        color: $text-muted;
        background: $surface;
        text-align: right;
    }
    """

    def __init__(
        self,
        job_id: str,
        log_path: Optional[str],
        log_content: Optional[Text],
        details: Optional[Dict[str, str]],
        job_ids: Optional[List[str]] = None,
        current_idx: int = 0,
        initial_pane: str = _DETAIL_PANE,
    ) -> None:
        super().__init__()
        self._job_id       = job_id
        self._log_path     = log_path or ''
        self._log_content  = log_content
        self._details      = details or {}
        self._job_ids      = job_ids or [job_id]
        self._idx          = current_idx
        self._log_cursor   = 0   # byte offset; 0 = not yet loaded incrementally
        self._initial_pane = initial_pane

    def compose(self) -> ComposeResult:
        """Compose the modal."""
        with Vertical(id='ji-body', classes='speek-popup'):
            with Horizontal(id='ji-main'):
                with ContentSwitcher(id='ji-switcher', initial=_DETAIL_PANE):
                    with Horizontal(id=_DETAIL_PANE):
                        with VerticalScroll(id='ji-detail-scroll'):
                            yield Static(id='ji-detail')
                        with VerticalScroll(id='ji-analysis-scroll'):
                            yield Static(id='ji-priority-content')
                    with Vertical(id=_OUTPUT_PANE):
                        yield Static(self._log_path, id='ji-log-path')
                        yield RichLog(id='ji-log', highlight=False, markup=False, wrap=False)
                    with Vertical(id=_GPU_PANE):
                        with Horizontal(id='ji-gpu-toolbar'):
                            yield Button('▶ Fetch  g', id='ji-gpu-fetch-btn')
                            yield Static('', id='ji-gpu-status', markup=True)
                        with VerticalScroll(id='ji-gpu-scroll'):
                            yield Static('Press [bold]g[/bold] to fetch live GPU stats.',
                                         id='ji-gpu-result', markup=True)
                with Vertical(id='ji-tab-sidebar'):
                    yield Static('1\nDetail',   id='ji-tab-btn-detail',   classes='ji-tab-btn')
                    yield Static('2\nOutput',   id='ji-tab-btn-output',   classes='ji-tab-btn')
                    yield Static('3\nGPU',      id='ji-tab-btn-gpu',      classes='ji-tab-btn')
            yield Static('', id='ji-hint', markup=True)

    def on_mount(self) -> None:
        self._set_active_tab(self._initial_pane)
        self._update_title()
        self._update_hint()
        self._populate_modal(self._log_path, self._log_content, self._details)
        self._load_analysis(self._job_id, self._details)

    def _populate_modal(
        self,
        log_path: str,
        log_content: Optional[Text],
        details: Dict[str, str],
    ) -> None:
        tv = self.app.theme_variables
        self.query_one('#ji-log-path', Static).update(log_path)
        log = self.query_one(_JI_LOG, RichLog)
        log.clear()
        if log_content is not None:
            _highlight_log(log_content)
            log.write(log_content)
        else:
            log.write(Text('No log available', style='dim'))
        self.query_one('#ji-detail', Static).update(_build_table(details, tv))

    def _update_title(self) -> None:
        title = _title_from_details(self._job_id, self._details)
        n = len(self._job_ids)
        if n > 1:
            title = f'[{self._idx + 1}/{n}]  {title}'
        self.query_one('#ji-body').border_title = title

    def _update_hint(self) -> None:
        n   = len(self._job_ids)
        nav = '[bold]h/l[/] job  ' if n > 1 else ''
        hint = (f'{nav}[bold]j/k[/] scroll  [bold]1/2/3[/] pane  '
                '[bold]g[/] fetch GPU  [bold]r[/] refresh  [bold]⇥[/] switch  [bold]q[/] close')
        self.query_one('#ji-hint', Static).update(hint)

    # ── Tab helpers ────────────────────────────────────────────────────────────

    _PANE_CYCLE = (_DETAIL_PANE, _OUTPUT_PANE, _GPU_PANE)

    def _set_active_tab(self, pane_id: str) -> None:
        # Map old pane id to detail if someone passes priority
        if pane_id == _PRIORITY_PANE:
            pane_id = _DETAIL_PANE
        self.query_one('#ji-switcher', ContentSwitcher).current = pane_id
        self.query_one('#ji-tab-btn-detail').set_class(pane_id == _DETAIL_PANE,   '--active')
        self.query_one('#ji-tab-btn-output').set_class(pane_id == _OUTPUT_PANE,   '--active')
        self.query_one('#ji-tab-btn-gpu').set_class(pane_id == _GPU_PANE,         '--active')

    def _active_pane(self) -> str:
        return self.query_one('#ji-switcher', ContentSwitcher).current or _DETAIL_PANE

    def action_switch_tab(self) -> None:
        cur = self._active_pane()
        nxt = self._PANE_CYCLE[(self._PANE_CYCLE.index(cur) + 1) % len(self._PANE_CYCLE)]
        self._set_active_tab(nxt)

    def action_show_detail(self) -> None:
        self._set_active_tab(_DETAIL_PANE)

    def action_show_output(self) -> None:
        self._set_active_tab(_OUTPUT_PANE)

    def action_show_gpu(self) -> None:
        self._set_active_tab(_GPU_PANE)

    def action_show_priority(self) -> None:
        self._set_active_tab(_DETAIL_PANE)

    # ── Sidebar click ─────────────────────────────────────────────────────────

    _TAB_BTN_MAP = {
        'ji-tab-btn-detail':   _DETAIL_PANE,
        'ji-tab-btn-output':   _OUTPUT_PANE,
        'ji-tab-btn-gpu':      _GPU_PANE,
    }

    def on_click(self, event) -> None:
        for node in event.widget.ancestors_with_self:
            if node.id in self._TAB_BTN_MAP:
                self._set_active_tab(self._TAB_BTN_MAP[node.id])
                event.stop()
                return

    # ── Fetch GPU ──────────────────────────────────────────────────────────────

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'ji-gpu-fetch-btn':
            self.action_fetch_gpu()

    def action_fetch_gpu(self) -> None:
        self._set_active_tab(_GPU_PANE)
        try:
            self.query_one('#ji-gpu-status', Static).update('[dim]fetching…[/dim]')
            self.query_one('#ji-gpu-result', Static).update('')
        except Exception:
            pass
        job_id   = self._job_id
        nodelist = self._details.get('NodeList', '') or self._details.get('BatchHost', '')

        def _worker():
            from speek.speek_max.slurm import fetch_job_gpu_stats
            return fetch_job_gpu_stats(job_id, nodelist)

        self.run_worker(_worker, thread=True, group='ji-gpu')

    # ── Scroll ─────────────────────────────────────────────────────────────────

    def _scroll_target(self):
        pane = self._active_pane()
        if pane == _OUTPUT_PANE:
            return self.query_one(_JI_LOG, RichLog)
        if pane == _GPU_PANE:
            return self.query_one('#ji-gpu-scroll', VerticalScroll)
        if pane == _PRIORITY_PANE:
            return self.query_one('#' + _PRIORITY_PANE, VerticalScroll)
        return self.query_one('#' + _DETAIL_PANE, VerticalScroll)

    def action_scroll_down(self) -> None:
        self._scroll_target().scroll_down(animate=False)

    def action_scroll_up(self) -> None:
        self._scroll_target().scroll_up(animate=False)

    def action_page_down(self) -> None:
        self._scroll_target().scroll_page_down(animate=False)

    def action_page_up(self) -> None:
        self._scroll_target().scroll_page_up(animate=False)

    # ── Job navigation ─────────────────────────────────────────────────────────

    def action_next_job(self) -> None:
        if self._idx < len(self._job_ids) - 1:
            self._idx += 1
            self._load_job(self._job_ids[self._idx])

    def action_prev_job(self) -> None:
        if self._idx > 0:
            self._idx -= 1
            self._load_job(self._job_ids[self._idx])

    def _load_job(self, job_id: str) -> None:
        self._job_id     = job_id
        self._log_cursor = 0
        self._update_hint()
        log = self.query_one(_JI_LOG, RichLog)
        log.clear()
        log.write(Text('Loading…', style='dim'))
        self.query_one('#ji-detail', Static).update('')
        n = len(self._job_ids)
        self.query_one('#ji-body').border_title = f'[{self._idx + 1}/{n}]  Job {job_id}  loading…'
        sacct_ok = (getattr(self.app, '_cmd_sacct', True)
                    and getattr(self.app, '_feat_sacct_details', True))
        self.run_worker(
            lambda jid=job_id, sa=sacct_ok: self._fetch(jid, sa),
            thread=True, group='ji-nav',
        )

    @staticmethod
    def _fetch(job_id: str, sacct_fallback: bool = True):
        """Single scontrol call → details + log path; log tail read concurrently."""
        from speek.speek_max.slurm import fetch_job_details_and_log_path
        from speek.speek_max.log_scan import scan_log_incremental

        with ThreadPoolExecutor(max_workers=2) as pool:
            # scontrol (details + log path) and an eager log-size probe run together.
            # The log read proper waits for the path, but the scontrol result is
            # cached so the second implicit call inside scan_log_incremental is free.
            details_fut = pool.submit(fetch_job_details_and_log_path, job_id, sacct_fallback)
            details, path = details_fut.result()
            # Now read the log tail while details are already in hand.
            content_fut = pool.submit(scan_log_incremental, path, 0, 500) if path else None
            if content_fut:
                content, cursor = content_fut.result()
            else:
                content, cursor = None, 0

        return path or '', content, details or {}, cursor

    @staticmethod
    def _fetch_incremental(job_id: str, log_path: str, cursor: int):
        """Refresh the same job: scontrol (cached) + only new log bytes."""
        from speek.speek_max.slurm import fetch_job_details
        from speek.speek_max.log_scan import scan_log_incremental

        with ThreadPoolExecutor(max_workers=2) as pool:
            details_fut = pool.submit(fetch_job_details, job_id)
            log_fut     = pool.submit(scan_log_incremental, log_path, cursor, 0) if log_path else None
            details = details_fut.result()
            if log_fut:
                new_text, new_cursor = log_fut.result()
            else:
                new_text, new_cursor = None, cursor

        return details or {}, new_text, new_cursor

    def _render_gpu(self, result: Dict) -> None:
        """Update the GPU pane with fetch_job_gpu_stats result."""
        from datetime import datetime
        ts     = datetime.now().strftime('%H:%M:%S')
        err    = result.get('error')
        sstat  = result.get('sstat', {})
        status = f'[dim]fetched {ts}[/dim]'
        if err:
            status += f'  [yellow]{err}[/yellow]'
        try:
            self.query_one('#ji-gpu-status', Static).update(status)
            self.query_one('#ji-gpu-result', Static).update(_build_gpu_table(result))
        except Exception:
            pass
        if sstat:
            parts = '  '.join(
                f'[dim]{k}[/dim] [bold]{v}[/bold]'
                for k, v in sstat.items() if v
            )
            try:
                scroll = self.query_one('#ji-gpu-scroll', VerticalScroll)
                existing = scroll.query('#ji-gpu-sstat')
                if existing:
                    existing.first(Static).update(parts)
                else:
                    scroll.mount(Static(parts, id='ji-gpu-sstat', markup=True))
            except Exception:
                pass

    def action_refresh(self) -> None:
        """Refresh details and append only new log bytes (incremental)."""
        job_id   = self._job_id
        log_path = self._log_path
        cursor   = self._log_cursor
        self.run_worker(
            lambda: self._fetch_incremental(job_id, log_path, cursor),
            thread=True, group='ji-refresh',
        )

    # ── Analysis pane (adaptive per job state) ──────────────────────────────────

    def _load_analysis(self, job_id: str, details: Dict) -> None:
        state = (details.get('JobState', '') or details.get('State', '')).split()[0].upper()
        user = getattr(self.app, 'user', '')

        def _worker():
            return self._fetch_analysis(job_id, state, user, details)

        self.run_worker(_worker, thread=True, group='ji-priority')

    @staticmethod
    def _fetch_analysis(job_id: str, state: str, user: str, details: Dict) -> Dict:
        """Fetch state-appropriate analysis data."""
        result: Dict = {'state': state, 'job_id': job_id}

        if state == 'PENDING':
            # Why is it pending? Priority scores + reason
            try:
                from speek.speek_max.slurm import fetch_priority_data
                result['priority'] = fetch_priority_data(job_id, user)
            except Exception:
                pass
            # Detailed priority factor breakdown
            try:
                from speek.speek_max.slurm import fetch_priority_factors
                result['priority_factors'] = fetch_priority_factors(job_id)
            except Exception:
                pass
            # User fairshare details
            try:
                from speek.speek_max.slurm import fetch_user_share
                result['user_share'] = fetch_user_share(user)
            except Exception:
                pass
            result['reason'] = details.get('Reason', '')

        elif state in ('FAILED', 'TIMEOUT', 'OUT_OF_MEMORY'):
            # Why did it fail? Exit code, node, stderr hints
            result['exit_code'] = details.get('ExitCode', details.get('DerivedExitCode', ''))
            result['reason'] = details.get('Reason', '')
            result['node'] = details.get('BatchHost', details.get('NodeList', ''))
            result['signal'] = details.get('DerivedExitCode', '')
            result['timelimit'] = details.get('Timelimit', details.get('TimeLimit', ''))
            result['elapsed'] = details.get('Elapsed', details.get('RunTime', ''))
            result['mem'] = details.get('MaxRSS', details.get('ReqMem', ''))

        elif state == 'RUNNING':
            # How is it going? Runtime, efficiency
            result['elapsed'] = details.get('Elapsed', details.get('RunTime', ''))
            result['timelimit'] = details.get('Timelimit', details.get('TimeLimit', ''))
            result['node'] = details.get('BatchHost', details.get('NodeList', ''))
            result['cpus'] = details.get('NumCPUs', details.get('AllocCPUS', ''))
            result['mem'] = details.get('ReqMem', '')
            # OOM scan
            try:
                from speek.speek_max.slurm import get_job_log_path
                from speek.speek_max.log_scan import detect_oom
                path = get_job_log_path(job_id)
                if path:
                    oom_msg = detect_oom(path)
                    if oom_msg:
                        result['oom_detected'] = oom_msg
            except Exception:
                pass

        elif state == 'COMPLETED':
            # Summary: duration, efficiency
            result['elapsed'] = details.get('Elapsed', details.get('RunTime', ''))
            result['timelimit'] = details.get('Timelimit', details.get('TimeLimit', ''))
            result['exit_code'] = details.get('ExitCode', '')
            result['node'] = details.get('BatchHost', details.get('NodeList', ''))
            # OOM scan
            try:
                from speek.speek_max.slurm import get_job_log_path
                from speek.speek_max.log_scan import detect_oom
                path = get_job_log_path(job_id)
                if path:
                    oom_msg = detect_oom(path)
                    if oom_msg:
                        result['oom_detected'] = oom_msg
            except Exception:
                pass

        elif state in ('CANCELLED',):
            result['reason'] = details.get('Reason', '')
            result['signal'] = details.get('DerivedExitCode', '')
            result['elapsed'] = details.get('Elapsed', details.get('RunTime', ''))

        return result

    @staticmethod
    def _render_priority_breakdown(
        pf: Optional[Dict], us: Optional[Dict], tv: Dict,
    ) -> List[str]:
        """Build markup lines for a detailed priority factor breakdown."""
        from speek.speek_max._utils import tc
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_warning = tc(tv, 'text-warning', 'yellow')

        lines: List[str] = ['', f'[bold {c_warning}]── Priority Breakdown ──[/]']

        if pf:
            lines.extend(JobInfoModal._fmt_priority_factors(pf, c_muted))
        if us:
            lines.extend(JobInfoModal._fmt_user_share(us, tv))

        return lines

    @staticmethod
    def _fmt_priority_factors(pf: Dict, c_muted: str) -> List[str]:
        """Format priority factor components into markup lines."""
        lines: List[str] = []
        total = float(pf.get('total', 0) or 0)
        lines.append(f'  [bold]Priority:[/bold]  {total:.0f}')
        for comp in pf.get('components', []):
            name = comp.get('name', '?')
            value = float(comp.get('value', 0) or 0)
            pct = (value / total * 100) if total else 0
            detail = comp.get('detail', '')
            pct_str = f'({pct:3.0f}%)' if total else ''
            line = f'  {name:<10} {value:>5.0f}  {pct_str}'
            if detail:
                line += f'  [{c_muted}]-- {detail}[/]'
            lines.append(line)
        queue_info = pf.get('queue_position')
        if queue_info:
            lines.extend(['', f'  [{c_muted}]{queue_info}[/]'])
        return lines

    @staticmethod
    def _fmt_user_share(us: Dict, tv: Dict) -> List[str]:
        """Format user share info into markup lines."""
        from speek.speek_max._utils import tc
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')
        c_error = tc(tv, 'text-error', 'red')

        lines: List[str] = ['']
        fs = us.get('fairshare')
        if fs is not None:
            if fs >= 0.5:
                fs_color = c_success
            elif fs >= 0.2:
                fs_color = c_warning
            else:
                fs_color = c_error
            lines.append(f'  [bold]Your usage:[/bold]  [{fs_color}]{fs:.3f} fairshare[/]')
        eff = us.get('effective_usage')
        alloc = us.get('fair_allocation')
        if eff is not None and alloc is not None:
            lines.append(f'  [{c_muted}]{eff:.1f}% effective vs {alloc:.1f}% share[/]')
        return lines

    def _render_analysis(self, data: Dict) -> None:
        from speek.speek_max._utils import tc
        tv = self.app.theme_variables
        c_muted = tc(tv, 'text-muted', 'bright_black')
        c_error = tc(tv, 'text-error', 'red')
        c_success = tc(tv, 'text-success', 'green')
        c_warning = tc(tv, 'text-warning', 'yellow')

        state = data.get('state', '')
        lines: list[str] = []

        if state == 'PENDING':
            lines.append(f'[bold {c_warning}]── Why Pending? ──[/]')
            reason = data.get('reason', '')
            if reason:
                lines.append(f'  [bold]Reason:[/bold]  {reason}')
            else:
                lines.append(f'  [{c_muted}]No reason reported[/]')

            # Detailed priority factor breakdown
            pf = data.get('priority_factors')
            us = data.get('user_share')
            if pf or us:
                lines.extend(self._render_priority_breakdown(pf, us, tv))
            else:
                # Fall back to legacy priority renderable
                prio = data.get('priority')
                if prio:
                    from speek.speek_max.widgets.priority_widget import build_priority_renderable
                    try:
                        self.query_one('#ji-priority-content', Static).update(
                            build_priority_renderable(prio, tv)
                        )
                        return
                    except Exception:
                        pass

        elif state in ('FAILED', 'TIMEOUT', 'OUT_OF_MEMORY'):
            label = {'FAILED': 'Failure', 'TIMEOUT': 'Timeout', 'OUT_OF_MEMORY': 'OOM'}
            lines.append(f'[bold {c_error}]── {label.get(state, state)} Analysis ──[/]')
            exit_code = data.get('exit_code', '')
            if exit_code and exit_code != '0:0':
                lines.append(f'  [bold]Exit code:[/bold]  [{c_error}]{exit_code}[/]')
            reason = data.get('reason', '')
            if reason:
                lines.append(f'  [bold]Reason:[/bold]    {reason}')
            node = data.get('node', '')
            if node:
                lines.append(f'  [bold]Node:[/bold]      {node}')
            elapsed = data.get('elapsed', '')
            timelimit = data.get('timelimit', '')
            if elapsed and timelimit:
                lines.append(f'  [bold]Runtime:[/bold]   {elapsed} / {timelimit}')
            mem = data.get('mem', '')
            if mem:
                lines.append(f'  [bold]Memory:[/bold]    {mem}')
            if state == 'TIMEOUT':
                lines.append(f'  [{c_muted}]Job exceeded its time limit[/]')
            elif state == 'OUT_OF_MEMORY':
                lines.append(f'  [{c_muted}]Job exceeded memory allocation[/]')
            # Extract errors from log
            if self._log_content:
                log_text = self._log_content.plain if hasattr(self._log_content, 'plain') else str(self._log_content)
                errors = _extract_errors(log_text)
                if errors:
                    lines.append('')
                    lines.append(f'  [bold {c_error}]── From Log ──[/]')
                    for label, text in errors[:5]:
                        lines.append(f'  [{c_warning}]{label}:[/]  [{c_error}]{text}[/]')

        elif state == 'RUNNING':
            oom_msg = data.get('oom_detected')
            if oom_msg:
                lines.append(f'[bold {c_error}]── ☢ OOM Detected ──[/]')
                lines.append(f'  [bold {c_error}]{oom_msg}[/]')
                lines.append('')
            lines.append(f'[bold {c_success}]── Running Status ──[/]')
            elapsed = data.get('elapsed', '')
            timelimit = data.get('timelimit', '')
            if elapsed and timelimit:
                lines.append(f'  [bold]Runtime:[/bold]   {elapsed} / {timelimit}')
            node = data.get('node', '')
            if node:
                lines.append(f'  [bold]Node:[/bold]      {node}')
            cpus = data.get('cpus', '')
            if cpus:
                lines.append(f'  [bold]CPUs:[/bold]      {cpus}')

        elif state == 'COMPLETED':
            oom_msg = data.get('oom_detected')
            if oom_msg:
                lines.append(f'[bold {c_error}]── ☢ OOM Detected ──[/]')
                lines.append(f'  [bold {c_error}]{oom_msg}[/]')
                lines.append(f'  [{c_warning}]Job completed but OOM errors found in log[/]')
                lines.append('')
            lines.append(f'[bold {c_success}]── Completed ──[/]')
            elapsed = data.get('elapsed', '')
            timelimit = data.get('timelimit', '')
            if elapsed:
                lines.append(f'  [bold]Runtime:[/bold]   {elapsed}' + (f' / {timelimit}' if timelimit else ''))
            exit_code = data.get('exit_code', '')
            if exit_code:
                color = c_success if exit_code == '0:0' else c_error
                lines.append(f'  [bold]Exit code:[/bold]  [{color}]{exit_code}[/]')
            node = data.get('node', '')
            if node:
                lines.append(f'  [bold]Node:[/bold]      {node}')

        elif state == 'CANCELLED':
            lines.append(f'[bold {c_warning}]── Cancelled ──[/]')
            reason = data.get('reason', '')
            if reason:
                lines.append(f'  [bold]Reason:[/bold]  {reason}')
            elapsed = data.get('elapsed', '')
            if elapsed:
                lines.append(f'  [bold]Runtime:[/bold] {elapsed}')

        else:
            lines.append(f'[dim]State: {state}[/dim]')

        if not lines:
            lines.append(f'[{c_muted}]No analysis available[/]')

        try:
            self.query_one('#ji-priority-content', Static).update('\n'.join(lines))
        except Exception:
            pass

    def on_worker_state_changed(self, event) -> None:
        from textual.worker import WorkerState
        if event.state != WorkerState.SUCCESS:
            return
        grp = event.worker.group
        if grp == 'ji-nav':
            path, content, details, cursor = event.worker.result
            self._log_path    = path
            self._log_content = content
            self._details     = details
            self._log_cursor  = cursor
            self._populate_modal(path, content, details)
            self._update_title()
            self._load_analysis(self._job_id, details)
        elif grp == 'ji-refresh':
            details, new_text, new_cursor = event.worker.result
            self._details    = details
            self._log_cursor = new_cursor
            self._update_title()
            # Append only new log lines (no clear)
            if new_text and new_text.plain:
                try:
                    self.query_one(_JI_LOG, RichLog).write(new_text)
                except Exception:
                    pass
            # Always refresh the detail pane
            try:
                tv = self.app.theme_variables
                self.query_one('#ji-detail', Static).update(_build_table(details, tv))
            except Exception:
                pass
        elif grp == 'ji-gpu':
            self._render_gpu(event.worker.result)
        elif grp == 'ji-priority':
            self._render_analysis(event.worker.result)
