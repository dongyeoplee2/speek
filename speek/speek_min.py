#!/usr/bin/env python3
"""speek-: one-shot cluster GPU overview — styled like speek-max's Cluster widget."""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from rich.align import Align
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text

# ── CLI ──────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description='speek-: cluster GPU snapshot')
parser.add_argument('-u', '--user', type=str, default=None)
args = parser.parse_args()

# ── Regex ────────────────────────────────────────────────────────────────────

_GPU_RE = re.compile(r'gpu:([A-Za-z0-9_\-]+):(\d+)', re.IGNORECASE)
_NODE_NUM_RE = re.compile(r'^(.*?)(\d+)$')

# ── Helpers ──────────────────────────────────────────────────────────────────

def _run(cmd: list[str]) -> str:
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return ''


def _usage_emoji(pct: float) -> str:
    if pct >= 100: return '💀'
    if pct > 90:   return '🔥'
    if pct == 0:   return '🏖'
    if pct < 10:   return '❄'
    return ''


def _util_color(pct: float) -> str:
    if pct >= 1.0:  return 'red'
    if pct >= 0.50: return 'yellow'
    return 'green'


def _bar(used: int, total: int, width: int = 16) -> Text:
    pct = used / total if total else 0.0
    if pct == 0:
        t = Text()
        t.append('|', style='bright_black')
        t.append(' 0% ', style='bold green')
        t.append(' ' * (width - 4) + '|', style='bright_black')
        return t
    color = _util_color(pct)
    fg = 'black' if pct < 0.90 else 'white'
    pct_str = f'{round(pct * 100)}%'
    fw = max(int(round(pct * width)), len(pct_str))
    aw = width - fw
    t = Text()
    t.append(f'|{pct_str:<{fw}}', style=f'bold {fg} on {color}')
    t.append(' ' * aw + '|', style='bright_black')
    return t


def _node_range(nodes: List[Tuple[str, str]]) -> Text:
    """Render [(name, state), ...] as compressed ranges like log-node1:3,5."""
    if not nodes:
        return Text('')
    parsed = []
    for name, state in nodes:
        m = _NODE_NUM_RE.match(name)
        if m:
            parsed.append((m.group(1), int(m.group(2)), state))
        else:
            parsed.append((name, -1, state))
    parsed.sort(key=lambda x: (x[0], x[1]))

    _STATE_COLORS = {
        'IDLE': 'green', 'ALLOCATED': 'red', 'MIXED': 'yellow',
        'DRAINING': 'red', 'DOWN': 'bright_black',
    }

    from itertools import groupby
    t = Text()
    prefix_groups: Dict[str, list] = {}
    for prefix, num, state in parsed:
        prefix_groups.setdefault(prefix, []).append((num, state))

    first = True
    for prefix, items in prefix_groups.items():
        runs: List[Tuple[int, int, str]] = []
        for num, state in items:
            if runs and runs[-1][2] == state and runs[-1][1] == num - 1:
                runs[-1] = (runs[-1][0], num, state)
            else:
                runs.append((num, num, state))
        if not first:
            t.append(' ', style='bright_black')
        t.append(prefix, style='bright_black')
        first = False
        for i, (start, end, state) in enumerate(runs):
            color = _STATE_COLORS.get(state, 'bright_black')
            if i:
                t.append(',', style='bright_black')
            if start == end:
                t.append(str(start), style=color)
            else:
                t.append(f'{start}:{end}', style=color)
    return t


# ── Data fetch ───────────────────────────────────────────────────────────────

_MODEL_VRAM: Dict[str, int] = {
    'H200': 141, 'H100': 80, 'A100': 80, 'A100-80GB': 80, 'A100-40GB': 40, '4A100': 40,
    'L40S': 48, 'L40': 48, 'A6000': 48, 'PRO6000': 48, 'A5000': 24,
    '3090': 24, '4090': 24, '2080ti': 11,
    'V100-32GB': 32, 'V100': 16, 'T4': 16,
}
_FLEX_RE = re.compile(r'gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', re.IGNORECASE)


def _fetch_cluster() -> Dict[str, Dict]:
    """Return {model: {Total, Used, Free, VRAM, CPUperGPU, RAMperGPU, Nodes}} from scontrol."""
    out = _run(['scontrol', 'show', 'node', '--oneliner'])
    if not out:
        return {}

    agg: Dict[str, Dict] = defaultdict(lambda: {
        'Total': 0, 'Used': 0, 'Nodes': [],
        '_cpu': 0, '_mem': 0,
    })
    seen: Dict[str, set] = defaultdict(set)

    for ln in out.splitlines():
        if not ln.strip():
            continue

        def _f(key: str) -> str:
            m = re.search(rf'{key}=(\S+)', ln)
            return m.group(1) if m else ''

        node = _f('NodeName')
        gres_fld = _f('Gres')
        cfg_tres = _f('CfgTRES')
        alloc_tres = _f('AllocTRES')
        gres_used = _f('GresUsed')
        state = _f('State').split('+')[0].rstrip('*~#$').upper()

        mm = _GPU_RE.search(gres_fld)
        model = mm.group(1) if mm else None
        if not model:
            continue

        # Total GPUs
        mg = re.search(r'gres/gpu=(\d+)', cfg_tres)
        total = int(mg.group(1)) if mg else (int(mm.group(2)) if mm else 0)
        if total == 0:
            continue

        # Used GPUs
        mu = re.search(r'gres/gpu=(\d+)', alloc_tres)
        if mu:
            used = int(mu.group(1))
        else:
            gu = _FLEX_RE.search(gres_used)
            used = int(gu.group(1)) if gu else 0
        used = min(used, total)

        # CPU and RAM
        try:
            cpu = int(_f('CPUTot') or 0)
        except ValueError:
            cpu = 0
        try:
            mem = int(_f('RealMemory') or 0)
        except ValueError:
            mem = 0

        agg[model]['Total'] += total
        agg[model]['Used'] += used
        agg[model]['_cpu'] += cpu
        agg[model]['_mem'] += mem
        if node not in seen[model]:
            seen[model].add(node)
            agg[model]['Nodes'].append((node, state))

    result: Dict[str, Dict] = {}
    for m, d in agg.items():
        n_gpus = d['Total'] or 1
        vram = _MODEL_VRAM.get(m)
        if vram is None:
            mv = re.search(r'(\d+)GB', m, re.IGNORECASE)
            vram = int(mv.group(1)) if mv else None
        result[m] = {
            'Total': d['Total'],
            'Used': d['Used'],
            'Free': d['Total'] - d['Used'],
            'VRAM': vram,
            'CPUperGPU': d['_cpu'] // n_gpus if d['_cpu'] else None,
            'RAMperGPU': d['_mem'] // n_gpus // 1024 if d['_mem'] else None,
            'Nodes': sorted(d['Nodes'], key=lambda x: x[0]),
        }
    return result


def _fetch_pending() -> Dict[str, int]:
    """Return {model: pending_job_count}."""
    out = _run(['squeue', '-t', 'PD', '-o', '%P|%b', '-h'])
    counts: Dict[str, int] = defaultdict(int)
    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + ['', ''])[:2]
        part, gres = parts
        m = _GPU_RE.search(gres or '')
        if m:
            counts[m.group(1)] += 1
        elif part.strip():
            counts[part.strip()] += 1
    return dict(counts)


# ── Render ───────────────────────────────────────────────────────────────────

def _display_width(s: str) -> int:
    """Terminal display width — accounts for wide chars (CJK, emoji)."""
    try:
        from unicodedata import east_asian_width
        w = 0
        for ch in s:
            cp = ord(ch)
            eaw = east_asian_width(ch)
            # Emoji (misc symbols, emoticons, etc.) are typically 2 cols
            if eaw in ('W', 'F') or cp >= 0x1F300:
                w += 2
            else:
                w += 1
        return w
    except Exception:
        return len(s)


def _col(t: Text, width: int) -> Text:
    """Pad a Text to exactly *width* display columns."""
    dw = _display_width(t.plain)
    if dw < width:
        t.append(' ' * (width - dw))
    return t


def _build_model_line(m: str, d: Dict, pending: Dict, show_nodes: bool = True) -> Text:
    """Build one GPU model row."""
    W_MODEL = 8   # model name
    W_VRAM  = 5   # vram like "48G"
    W_BAR   = 16
    W_CNT   = 6
    W_DEM   = 4

    T, U, F = d['Total'], d['Used'], d['Free']
    pct = U / T if T else 0.0
    uc = _util_color(pct)

    model_t = Text()
    model_t.append(m, style=f'bold {uc}')

    vram = d.get('VRAM')
    vram_t = Text(f'{vram}G', style='bright_black') if vram else Text('')

    cnt_t = Text()
    cnt_t.append(f'{F}', style=f'bold {uc}')
    cnt_t.append(f'/{T}', style='bright_black')

    pd = pending.get(m, 0)
    dem_t = Text()
    if pd:
        pressure = pd / max(F, 1)
        dc = 'red' if pressure >= 2 else ('yellow' if pressure >= 1 else 'bright_black')
        dem_t.append(f'↑{pd}', style=dc)

    emoji = _usage_emoji(pct * 100)

    line = Text()
    line.append_text(_col(model_t, W_MODEL))
    line.append_text(_col(vram_t, W_VRAM))
    line.append_text(_col(Text(emoji), 4))
    line.append_text(_bar(U, T, W_BAR))
    line.append(' ')
    line.append_text(_col(cnt_t, W_CNT))
    line.append_text(_col(dem_t, W_DEM))

    if show_nodes:
        nodes = d.get('Nodes', [])
        line.append_text(_col(Text(f'{len(nodes)}×', style='bright_black'), 3))
        line.append_text(_node_range(nodes))

    return line


def _line_width(show_nodes: bool) -> int:
    """Approximate char width of one model line."""
    return 47 if not show_nodes else 60


def build_panel(stats: Dict[str, Dict], term_w: int = 80, term_h: int = 24) -> Panel:
    """Build a Rich Panel, auto-arranging into multi-column when terminal is wide but short."""
    if not stats:
        return Panel(
            Text('No GPU data', style='bright_black italic'),
            title='[bold]speek-[/bold]',
            border_style='bright_black',
        )

    pending = _fetch_pending()
    models = sorted(stats, key=lambda m: stats[m]['Total'], reverse=True)
    n = len(models)

    # Decide layout: how many columns fit?
    # Each column needs ~45 chars (no nodes) or ~60 chars (with nodes)
    # Panel border + padding = ~6 chars
    usable_w = term_w - 6
    # Available rows for GPU models (panel border=2, total row=1)
    usable_h = term_h - 4

    # Try with nodes first, then without
    col_w_nodes = _line_width(True)
    col_w_compact = _line_width(False)

    if n <= usable_h:
        # Everything fits in one column
        n_cols = 1
        show_nodes = True
    elif n <= usable_h * (usable_w // col_w_nodes):
        # Multi-column with nodes
        n_cols = min(usable_w // col_w_nodes, (n + usable_h - 1) // usable_h)
        n_cols = max(1, n_cols)
        show_nodes = True
    else:
        # Multi-column without nodes (compact)
        n_cols = min(usable_w // col_w_compact, (n + usable_h - 1) // usable_h)
        n_cols = max(1, n_cols)
        show_nodes = False

    # Build model lines
    model_lines = []
    total_T = total_U = 0
    for m in models:
        d = stats[m]
        total_T += d['Total']
        total_U += d['Used']
        model_lines.append(_build_model_line(m, d, pending, show_nodes))

    # Arrange into columns
    rows_per_col = (n + n_cols - 1) // n_cols
    sep = Text('  │ ', style='bright_black')

    output_lines: List[Text] = []
    for row_i in range(rows_per_col):
        line = Text()
        for col_i in range(n_cols):
            idx = col_i * rows_per_col + row_i
            if col_i > 0:
                line.append_text(sep)
            if idx < n:
                ml = model_lines[idx]
                # Pad to fixed width for alignment across columns
                target_w = col_w_nodes if show_nodes else col_w_compact
                pad = max(0, target_w - len(ml.plain))
                line.append_text(ml)
                if pad > 0:
                    line.append(' ' * pad)
            else:
                target_w = col_w_nodes if show_nodes else col_w_compact
                line.append(' ' * target_w)
        output_lines.append(line)

    # Total row with dark gray background spanning full width
    total_pct = total_U / total_T if total_T else 0.0
    uc = _util_color(total_pct)
    bg = 'on #2a2a2a'
    total_line = Text(style=bg)
    tname = Text()
    tname.append('Total', style=f'bold {uc} {bg}')
    tcnt = Text()
    tcnt.append(f'{total_T - total_U}', style=f'bold {uc} {bg}')
    tcnt.append(f'/{total_T}', style=f'bright_black {bg}')
    total_line.append_text(_col(tname, 8))    # W_MODEL
    total_line.append_text(_col(Text('', style=bg), 5))  # W_VRAM
    total_line.append_text(_col(Text(_usage_emoji(total_pct * 100), style=bg), 4))
    total_line.append_text(_bar(total_U, total_T, 16))
    total_line.append(' ', style=bg)
    total_line.append_text(_col(tcnt, 6))
    # Pad to match the widest model line
    row_w = _display_width(output_lines[0].plain) if output_lines else 60
    cur_w = _display_width(total_line.plain)
    if cur_w < row_w:
        total_line.append(' ' * (row_w - cur_w), style=bg)
    output_lines.append(total_line)

    content = Text('\n').join(output_lines)

    return Panel(
        content,
        title='[bold]speek-[/bold] [dim]v0.0.3[/dim]',
        subtitle='[dim]Cluster[/dim]',
        border_style='bright_blue',
        padding=(0, 1),
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    console = Console()
    w, h = console.size
    stats = _fetch_cluster()
    panel = build_panel(stats, term_w=w, term_h=h)
    console.print()
    console.print(Align(panel, align='center'))
    console.print()


if __name__ == '__main__':
    main()
