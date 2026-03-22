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


# ── Error classification ───────────────────────────────────────────────────

_ERROR_PATTERNS: list[tuple[re.Pattern, str, str, list[str]]] = [
    # (regex, error_type, description, suggestions)
    (re.compile(r'(?:CUDA out of memory|OutOfMemoryError|torch\.cuda\.OutOfMemoryError|'
                r'oom-kill|Cannot allocate memory|CUDA error: out of memory)', re.I),
     'OOM', 'GPU out of memory',
     ['Reduce batch size', 'Use gradient checkpointing', 'Use mixed precision (fp16/bf16)',
      'Check for memory leaks between steps']),

    (re.compile(r'NCCL\s*(?:error|timeout|warn)|ncclInternalError|ncclSystemError|'
                r'NCCL_ASYNC_ERROR_HANDLING', re.I),
     'NCCL', 'Multi-GPU communication failure',
     ['Check network between nodes (ibstat/ibstatus)', 'Set NCCL_DEBUG=INFO for details',
      'Try NCCL_P2P_DISABLE=1 or NCCL_IB_DISABLE=1',
      'Ensure all GPUs are visible (CUDA_VISIBLE_DEVICES)']),

    (re.compile(r'RuntimeError:\s*(?:CUDA|cuda).*(?:device-side assert|illegal memory access|'
                r'an illegal instruction|misaligned address)', re.I),
     'CUDA_ERROR', 'CUDA runtime error',
     ['Run with CUDA_LAUNCH_BLOCKING=1 for exact location',
      'Check for out-of-bounds indexing in custom CUDA kernels',
      'Verify tensor shapes and dtypes']),

    (re.compile(r'(?:NaN|nan)\s*(?:loss|detected|in gradient|in forward)|'
                r'loss.*(?:nan|inf)|grad.*(?:nan|inf)|'
                r'FloatingPointError', re.I),
     'NAN_LOSS', 'NaN/Inf detected in training',
     ['Lower learning rate', 'Add gradient clipping (max_norm=1.0)',
      'Check data preprocessing for invalid values',
      'Use loss scaling with mixed precision']),

    (re.compile(r'(?:size mismatch|shape.*mismatch|dimension.*mismatch|'
                r'mat1 and mat2 shapes cannot be multiplied|'
                r'Expected.*got.*size|RuntimeError:.*size)', re.I),
     'SHAPE_MISMATCH', 'Tensor shape mismatch',
     ['Check model input dimensions', 'Verify data loader output shapes',
      'Check that pretrained weights match model architecture']),

    (re.compile(r'FileNotFoundError|No such file or directory|IsADirectoryError|'
                r'PermissionError.*denied', re.I),
     'FILE_ERROR', 'File system error',
     ['Check file paths exist', 'Verify read/write permissions',
      'Check shared filesystem mount (NFS/Lustre)']),

    (re.compile(r'(?:torch\.distributed|dist\.).*(?:timeout|timed out|barrier)|'
                r'ProcessGroupNCCL.*timeout|'
                r'Timed out initializing process group', re.I),
     'DIST_TIMEOUT', 'Distributed training timeout',
     ['Increase timeout (dist.init_process_group timeout=)',
      'Check that all ranks launched correctly',
      'Verify MASTER_ADDR and MASTER_PORT',
      'Check for straggler GPUs or uneven data']),

    (re.compile(r'Segmentation fault|SIGSEGV|core dumped|signal 11', re.I),
     'SEGFAULT', 'Segmentation fault',
     ['Check for corrupted data files', 'Update PyTorch/CUDA versions',
      'Check custom C++/CUDA extensions',
      'Try reducing num_workers in DataLoader']),

    (re.compile(r'SIGTERM|preempted|slurmstepd.*SIGTERM', re.I),
     'PREEMPTED', 'Job was preempted/terminated',
     ['Use checkpointing to resume from last saved state',
      'Request a non-preemptible partition',
      'Reduce walltime to avoid preemption window']),

    (re.compile(r'ImportError|ModuleNotFoundError|No module named', re.I),
     'IMPORT_ERROR', 'Missing Python module',
     ['Check conda/pip environment is activated',
      'Install missing package', 'Verify PYTHONPATH']),

    (re.compile(r'SIGKILL|Killed|signal 9', re.I),
     'KILLED', 'Process killed (likely system OOM)',
     ['Check system (CPU) memory usage — not just GPU',
      'Reduce num_workers in DataLoader',
      'Request more memory (--mem or --mem-per-cpu)']),
]


def classify_error(log_path: str, tail_lines: int = 500) -> Optional[dict]:
    """Classify the error in a job log.

    Returns dict with keys:
        error_type: str — short error code
        description: str — human-readable description
        trigger: str — the actual error line from log
        stack_frames: list[str] — user code stack trace
        suggestions: list[str] — actionable fixes
        context_lines: list[str] — lines around the error
    Returns None if no known error pattern found.
    """
    try:
        p = Path(log_path)
        if not p.exists():
            return None
        size = p.stat().st_size
        if size == 0:
            return None
        chunk = min(size, tail_lines * 200)
        with open(p, 'r', errors='replace') as f:
            if size > chunk:
                f.seek(size - chunk)
                f.readline()
            lines = f.readlines()[-tail_lines:]
    except Exception:
        return None

    # Find first matching error pattern (scan from bottom for most recent)
    best_idx = -1
    best_match = None
    for i in range(len(lines) - 1, -1, -1):
        for pat, etype, desc, suggestions in _ERROR_PATTERNS:
            if pat.search(lines[i]):
                if best_idx == -1 or i > best_idx:
                    best_idx = i
                    best_match = (etype, desc, suggestions, lines[i].strip()[:120])
                break
        if best_match:
            break

    if not best_match:
        return None

    etype, desc, suggestions, trigger = best_match

    # Extract stack frames near the error
    frames = []
    for ln in lines[max(0, best_idx - 30):best_idx]:
        fm = _STACK_FRAME_RE.search(ln)
        if fm:
            filepath = fm.group(1)
            if 'site-packages' not in filepath and 'lib/python' not in filepath:
                frames.append(f'{filepath}:{fm.group(2)}')

    # Context lines around the error
    ctx_start = max(0, best_idx - 3)
    ctx_end = min(len(lines), best_idx + 3)
    context = [ln.rstrip() for ln in lines[ctx_start:ctx_end]]

    return {
        'error_type': etype,
        'description': desc,
        'trigger': trigger,
        'stack_frames': frames[-5:],
        'suggestions': list(suggestions),
        'context_lines': context,
    }


# ── Deep OOM analysis ──────────────────────────────────────────────────────

_CUDA_MEM_RE = re.compile(
    r'Tried to allocate ([\d.]+)\s*(\w+).*?([\d.]+)\s*(\w+)\s*(?:already\s+)?allocated',
    re.IGNORECASE,
)
_GPU_TOTAL_RE = re.compile(
    r'total.?(?:memory|capacity)[:\s]*([\d.]+)\s*(\w+)',
    re.IGNORECASE,
)
_BATCH_SIZE_RE = re.compile(
    r'batch.?size[=:\s]+(\d+)',
    re.IGNORECASE,
)
_STACK_FRAME_RE = re.compile(
    r'File "([^"]+)", line (\d+)',
)


def analyze_oom(log_path: str, tail_lines: int = 500) -> Optional[dict]:
    """Deep OOM analysis — extract memory details, stack trace, and suggestions.

    Returns dict with keys:
        trigger: str — the OOM error line
        tried_alloc: str — attempted allocation size
        already_alloc: str — already allocated
        gpu_total: str — total GPU memory (if found)
        batch_size: str — detected batch size (if found)
        stack_frames: list[str] — relevant stack trace lines
        suggestions: list[str] — actionable suggestions
    Returns None if no OOM found.
    """
    try:
        p = Path(log_path)
        if not p.exists():
            return None
        size = p.stat().st_size
        if size == 0:
            return None
        chunk = min(size, tail_lines * 200)
        with open(p, 'r', errors='replace') as f:
            if size > chunk:
                f.seek(size - chunk)
                f.readline()
            lines = f.readlines()[-tail_lines:]
    except Exception:
        return None

    # Find OOM trigger line
    trigger_idx = None
    trigger_line = ''
    for i, ln in enumerate(lines):
        if _OOM_RE.search(ln):
            trigger_idx = i
            trigger_line = ln.strip()

    if trigger_idx is None:
        return None

    result: dict = {'trigger': trigger_line[:120]}

    # Extract memory details from surrounding lines
    context = lines[max(0, trigger_idx - 10):trigger_idx + 5]
    context_text = ''.join(context)

    m = _CUDA_MEM_RE.search(context_text)
    if m:
        result['tried_alloc'] = f'{m.group(1)} {m.group(2)}'
        result['already_alloc'] = f'{m.group(3)} {m.group(4)}'

    m = _GPU_TOTAL_RE.search(context_text)
    if m:
        result['gpu_total'] = f'{m.group(1)} {m.group(2)}'

    # Scan full text for batch size
    full_text = ''.join(lines)
    m = _BATCH_SIZE_RE.search(full_text)
    if m:
        result['batch_size'] = m.group(1)

    # Extract stack frames near the OOM
    frames = []
    for ln in lines[max(0, trigger_idx - 30):trigger_idx]:
        fm = _STACK_FRAME_RE.search(ln)
        if fm:
            filepath = fm.group(1)
            if 'site-packages' not in filepath and 'lib/python' not in filepath:
                frames.append(f'{filepath}:{fm.group(2)}')
    result['stack_frames'] = frames[-5:]

    # Generate suggestions
    suggestions = []
    if 'tried_alloc' in result:
        suggestions.append('Reduce batch size or model size to lower memory allocation')
    if 'batch_size' in result:
        bs = int(result['batch_size'])
        suggestions.append(f'Try batch_size={bs // 2} (halved from {bs})')
    suggestions.append('Use gradient checkpointing (torch.utils.checkpoint)')
    suggestions.append('Use mixed precision training (torch.cuda.amp)')
    if 'already_alloc' in result:
        suggestions.append('Check for memory leaks — tensors not freed between steps')
    result['suggestions'] = suggestions

    return result
