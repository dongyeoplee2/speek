"""log_scan.py — Tail a job log and highlight training/error patterns."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Tuple

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

_OOM_RE = re.compile(
    r'(?:out of memory|CUDA out of memory|OutOfMemoryError|'
    r'torch\.cuda\.OutOfMemoryError|CUDA error: out of memory|'
    r'oom-kill|Cannot allocate memory|OOM|'
    r'Killed|SIGKILL)',
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


def scan_log(log_path: str, tail: Optional[int] = None) -> Optional[Text]:
    """Return a Rich Text block of the log, patterns highlighted.

    tail=None (default) reads the full file. tail=N reads the last N lines.
    """
    try:
        p = Path(log_path)
        if not p.exists():
            return None
        lines = p.read_text(errors='replace').splitlines()
        if tail:
            lines = lines[-tail:]
        result = Text()
        for i, ln in enumerate(lines):
            if i:
                result.append('\n')
            result.append_text(_style_line(ln))
        return result
    except Exception as e:
        return Text(f'Could not read log: {e}', style='red')


def scan_log_incremental(
    log_path: str,
    start_byte: int = 0,
    tail: int = 500,
) -> Tuple[Optional[Text], int]:
    """Read a log file and return (highlighted_text, end_byte_offset).

    start_byte=0 (first open): reads the last `tail` lines; returns file size
        as the cursor for the next incremental call.
    start_byte>0 (refresh): reads only bytes from start_byte to EOF and appends
        them to the existing view.  Returns (empty Text, start_byte) when there
        is nothing new, so callers can detect a no-op.
    Returns (None, 0) when the file does not exist or on read error.
    """
    try:
        p = Path(log_path)
        if not p.exists():
            return None, 0
        file_size = p.stat().st_size
        if start_byte > 0:
            if file_size <= start_byte:
                return Text(), start_byte          # nothing new
            with p.open('rb') as f:
                f.seek(start_byte)
                chunk = f.read(file_size - start_byte)
            lines = chunk.decode('utf-8', errors='replace').splitlines()
        else:
            lines = p.read_text(errors='replace').splitlines()
            if tail and len(lines) > tail:
                lines = lines[-tail:]
        result = Text()
        for i, ln in enumerate(lines):
            if i or start_byte > 0:
                result.append('\n')
            result.append_text(_style_line(ln))
        return result, file_size
    except Exception as e:
        return Text(f'Could not read log: {e}', style='red'), 0


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


# Cache: (path, file_size) → result (str or None)
# Once OOM is found (truthy), it stays cached permanently for that path.
# None results are re-checked when file grows.
_oom_cache: dict[str, tuple[int, Optional[str]]] = {}


def detect_oom(log_path: str, tail_lines: int = 200) -> Optional[str]:
    """Scan the last *tail_lines* of a log for OOM signals.

    Returns a short description if OOM is detected, None otherwise.
    Results are cached by (path, size) — a truthy result is permanent,
    a None result is re-checked when the file grows.
    """
    try:
        p = Path(log_path)
        if not p.exists():
            return None
        size = p.stat().st_size
        if size == 0:
            return None
        # Check cache
        cached = _oom_cache.get(log_path)
        if cached is not None:
            cached_size, cached_result = cached
            if cached_result is not None:
                return cached_result  # OOM found before — permanent
            if cached_size == size:
                return None  # file unchanged, still no OOM
        # Scan tail
        chunk = min(size, tail_lines * 200)
        with open(p, 'r', errors='replace') as f:
            if size > chunk:
                f.seek(size - chunk)
                f.readline()
            lines = f.readlines()[-tail_lines:]
        result = None
        for ln in lines:
            m = _OOM_RE.search(ln)
            if m:
                result = ln.strip()[:60]
                break
        _oom_cache[log_path] = (size, result)
        return result
    except Exception:
        return None
