"""log_scan.py — Tail a job log and highlight training/error patterns."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from rich.text import Text

# ── Patterns ──────────────────────────────────────────────────────────────────

_PROGRESS_RE = re.compile(
    r'(?:'
    r'step\s*[:\s]\s*(\d+)[/\s]*(\d+)?'        # step 42/1000
    r'|epoch\s*[:\s]\s*([\d.]+)'                # epoch 3
    r'|loss\s*[:\s=]\s*([\d.eE+\-]+)'          # loss: 0.342
    r'|\[(\d+)/(\d+)\]'                         # [42/1000] tqdm style
    r'|(\d+)%\|'                                # 42%| tqdm bar
    r'|it/s|s/it'                               # tqdm speed
    r')',
    re.IGNORECASE,
)

_ERROR_RE = re.compile(
    r'(?:OOM|out of memory|CUDA error|RuntimeError|Traceback|Error:|assert|'
    r'killed|segfault|core dumped|SIGKILL|SIGTERM|slurmstepd)',
    re.IGNORECASE,
)

_WARN_RE = re.compile(
    r'(?:warning|warn:|UserWarning|DeprecationWarning)',
    re.IGNORECASE,
)

_WANDB_RE = re.compile(r'wandb:', re.IGNORECASE)


def _style_line(line: str) -> Text:
    t = Text(line, no_wrap=True, overflow='fold')
    if _ERROR_RE.search(line):
        t.stylize('bold red')
    elif _PROGRESS_RE.search(line):
        t.stylize('green')
    elif _WANDB_RE.search(line):
        t.stylize('bright_yellow')
    elif _WARN_RE.search(line):
        t.stylize('yellow')
    return t


def scan_log(log_path: str, tail: int = 40) -> Optional[Text]:
    """Return a Rich Text block of the last `tail` lines, patterns highlighted."""
    try:
        p = Path(log_path)
        if not p.exists():
            return None
        lines = p.read_text(errors='replace').splitlines()[-tail:]
        result = Text()
        for i, ln in enumerate(lines):
            if i:
                result.append('\n')
            result.append_text(_style_line(ln))
        return result
    except Exception as e:
        return Text(f'Could not read log: {e}', style='red')


def extract_hint(log_path: str) -> Optional[str]:
    """Return a short one-line hint from the last 20 lines of the log."""
    try:
        p = Path(log_path)
        if not p.exists():
            return None
        lines = p.read_text(errors='replace').splitlines()[-20:]
        # scan from bottom up for the most recent signal
        for ln in reversed(lines):
            if _ERROR_RE.search(ln):
                snip = ln.strip()[:40]
                return f'⚠ {snip}'
            if _PROGRESS_RE.search(ln):
                snip = ln.strip()[:40]
                return snip
        return None
    except Exception:
        return None
