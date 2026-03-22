#!/usr/bin/env python3
"""speek-: one-shot cluster GPU overview — styled like speek-max's Cluster widget."""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.align import Align
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table as RichTable
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
    if pct < 10:   return '❄️'
    return ''


def _util_color(pct: float) -> str:
    if pct >= 1.0:  return 'red'
    if pct >= 0.50: return 'yellow'
    return 'green'


# ── Trend tracking ──────────────────────────────────────────────────────────
# Keeps a rolling history of {model: used_gpus} snapshots (~30 min window).
# Always compares current state to ~5 min ago — gives a stable, meaningful
# trend regardless of how often you run speek-.
#
# Use case: "Should I hurry to submit?" → ↓3 on A100 means 3 GPUs were
# taken in the last 5 min — competition is high, submit now.
# ↑2 means 2 freed up — no rush.

_TREND_FILE = Path(os.environ.get('XDG_CACHE_HOME', Path.home() / '.cache')) / 'speek' / 'usage_trend.json'
_TREND_WINDOW = 300     # compare to ~5 minutes ago
_TREND_MAX_HISTORY = 60  # keep max 60 snapshots (~30 min at 30s intervals)


def _load_history() -> List[Dict]:
    """Load snapshot history. Returns [{ts: float, used: {model: int}}, ...]."""
    try:
        data = json.loads(_TREND_FILE.read_text())
        if isinstance(data, list):
            return data
        return []  # old format — discard
    except Exception:
        return []


def _save_history(history: List[Dict]) -> None:
    try:
        _TREND_FILE.parent.mkdir(parents=True, exist_ok=True)
        _TREND_FILE.write_text(json.dumps(history))
    except Exception:
        pass


def _compute_trends(stats: Dict[str, Dict]) -> Dict[str, Tuple[str, int]]:
    """Compare current usage to ~5 min ago. Returns {model: (arrow, delta)}.

    Keeps a rolling history of snapshots. Finds the snapshot closest to
    5 minutes ago and compares. Shows stable trends even when checking
    every few seconds.
    """
    now = time.time()
    current_used = {m: d['Used'] for m, d in stats.items()}

    # Load and append current snapshot
    history = _load_history()
    history.append({'ts': now, 'used': current_used})

    # Prune: keep only last 30 min, max 60 entries
    history = [h for h in history if now - h['ts'] < 1800]
    if len(history) > _TREND_MAX_HISTORY:
        history = history[-_TREND_MAX_HISTORY:]

    _save_history(history)

    # Find snapshot closest to 5 min ago
    target = now - _TREND_WINDOW
    best = None
    best_dist = float('inf')
    for h in history[:-1]:  # exclude current
        dist = abs(h['ts'] - target)
        if dist < best_dist:
            best_dist = dist
            best = h

    # Need a snapshot within reasonable range (2-10 min ago)
    if best is None or best_dist > _TREND_WINDOW * 2:
        return {}

    prev_used = best['used']
    trends: Dict[str, Tuple[str, int]] = {}
    for m, cur_u in current_used.items():
        old_u = prev_used.get(m)
        if old_u is None:
            continue
        delta = cur_u - old_u  # positive = more used = less free
        if delta > 0:
            trends[m] = ('↓', delta)   # less free — hurry
        elif delta < 0:
            trends[m] = ('↑', -delta)  # more free — no rush
    return trends


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


def _build_model_line(m: str, d: Dict, pending: Dict, my_gpus: Dict,
                      show_nodes: bool = True,
                      trend: Tuple[str, int] = None,
                      w_nodes: int = 14,
                      has_any_trend: bool = False,
                      col_widths: Dict[str, int] = None,
                      align_w: int = 0) -> Text:
    """Build one GPU model row."""
    cw = col_widths or {}
    W_MODEL = cw.get('model', 8)
    W_VRAM  = cw.get('vram', 4)
    W_BAR   = cw.get('bar', 14)
    W_CNT   = cw.get('cnt', 5)
    W_DEM   = cw.get('dem', 3)
    W_MY    = cw.get('my', 6)

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
        dem_t.append(f'⏸{pd}', style=dc)

    # Trend: availability change since last check
    trend_t = Text()
    if trend:
        arrow, delta = trend
        if arrow == '↑':
            trend_t.append(f'{arrow}{delta}', style='bold green')
        else:
            trend_t.append(f'{arrow}{delta}', style='bold red')

    emoji = _usage_emoji(pct * 100)

    line = Text()
    line.append_text(_col(model_t, W_MODEL))
    line.append_text(_col(vram_t, W_VRAM))
    # Emoji appended inline — no fixed-width column (avoids terminal width quirks)
    # Emoji slot: always exactly 3 chars in len() and 3 terminal cells.
    # Emoji = 1 char (2 cells) → pad to 3 chars with 2 spaces (total 4 cells)
    # No emoji = 0 chars → pad to 3 chars with 3 spaces (total 3 cells)
    # Difference: emoji rows are 1 cell wider. But since we use len() for
    # padding before │, the extra visual cell is absorbed.
    # Actually: just make len() identical for both paths.
    emoji_t = Text(emoji if emoji else '')
    line.append_text(_col(emoji_t, 4))
    line.append_text(_bar(U, T, W_BAR))
    line.append_text(_col(cnt_t, W_CNT))
    line.append_text(_col(dem_t, W_DEM))
    if has_any_trend:
        line.append_text(_col(trend_t, 3))

    W_MYJOB = 6

    if show_nodes:
        nodes = d.get('Nodes', [])
        node_t = Text()
        if len(nodes) > 1:
            node_t.append(f'{len(nodes)}×', style='bright_black')
            node_t.append(' ')
        node_t.append_text(_node_range(nodes))
        line.append_text(_col(node_t, w_nodes))
    else:
        line.append_text(_col(Text(''), w_nodes))

    # Return (left_line, my_jobs_text) — build_panel aligns │ after measuring all lines
    mg = my_gpus.get(m, {})
    my_r, my_pd = mg.get('R', 0), mg.get('PD', 0)
    my_t = Text()
    if my_r:
        my_t.append(f'▶{my_r}', style='bold green')
    if my_pd:
        if my_r:
            my_t.append(' ')
        my_t.append(f'⏸{my_pd}', style='bold yellow')

    return line, my_t


def _line_width(show_nodes: bool) -> int:
    """Approximate char width of one model line."""
    return 43 if not show_nodes else 57


def build_panel(stats: Dict[str, Dict], my_gpus: Dict,
                term_w: int = 80, term_h: int = 24, user: str = '') -> Panel:
    """Build a Rich Panel, auto-arranging into multi-column when terminal is wide but short."""
    if not stats:
        return Panel(
            Text('No GPU data', style='bright_black italic'),
            title='[bold]speek-[/bold]',
            border_style='bright_black',
        )

    pending = _fetch_pending()
    trends = _compute_trends(stats)
    models = sorted(stats, key=lambda m: stats[m]['Total'], reverse=True)
    has_any_trend = bool(trends)

    # Build table
    table = RichTable(show_header=False, show_edge=False, box=None,
                      pad_edge=False, padding=(0, 1, 0, 0))
    table.add_column('model', style='bold', no_wrap=True)
    table.add_column('vram', style='bright_black', no_wrap=True)
    table.add_column('emoji', no_wrap=True, width=2)
    table.add_column('bar', no_wrap=True)
    table.add_column('free', no_wrap=True)
    table.add_column('dem', no_wrap=True)
    if has_any_trend:
        table.add_column('trend', no_wrap=True)
    table.add_column('sep', no_wrap=True, width=1)
    table.add_column('my', no_wrap=True)

    total_T = total_U = 0
    for m in models:
        d = stats[m]
        T, U, F = d['Total'], d['Used'], d['Free']
        total_T += T
        total_U += U
        pct = U / T if T else 0.0
        uc = _util_color(pct)

        emoji = _usage_emoji(pct * 100)
        vram = d.get('VRAM')

        cnt_t = Text()
        cnt_t.append(f'{F}', style=f'bold {uc}')
        cnt_t.append(f'/{T}', style='bright_black')

        pd = pending.get(m, 0)
        dem_t = Text()
        if pd:
            pressure = pd / max(F, 1)
            dc = 'red' if pressure >= 2 else ('yellow' if pressure >= 1 else 'bright_black')
            dem_t.append(f'⏸{pd}', style=dc)

        trend_t = Text()
        trend = trends.get(m)
        if trend:
            arrow, delta = trend
            trend_t.append(f'{arrow}{delta}', style='bold green' if arrow == '↑' else 'bold red')

        mg = my_gpus.get(m, {})
        my_r, my_pd = mg.get('R', 0), mg.get('PD', 0)
        my_t = Text()
        if my_r:
            my_t.append(f'▶{my_r}', style='bold green')
        if my_pd:
            if my_r:
                my_t.append(' ')
            my_t.append(f'⏸{my_pd}', style='bold yellow')

        row = [
            Text(m, style=f'bold {uc}'),
            Text(f'{vram}G', style='bright_black') if vram else Text(''),
            Text(emoji),
            _bar(U, T, 14),
            cnt_t,
            dem_t,
        ]
        if has_any_trend:
            row.append(trend_t)
        row.append(Text('│', style='bright_black'))
        row.append(my_t)
        table.add_row(*row)

    # Total row
    total_pct = total_U / total_T if total_T else 0.0
    uc = _util_color(total_pct)
    bg = 'on #2a2a2a'
    total_emoji = _usage_emoji(total_pct * 100)
    tcnt = Text()
    tcnt.append(f'{total_T - total_U}', style=f'bold {uc} {bg}')
    tcnt.append(f'/{total_T}', style=f'bright_black {bg}')
    total_my_r = sum(v.get('R', 0) for v in my_gpus.values())
    total_my_pd = sum(v.get('PD', 0) for v in my_gpus.values())
    my_total = Text()
    if total_my_r:
        my_total.append(f'▶{total_my_r}', style='bold green')
    if total_my_pd:
        if total_my_r:
            my_total.append(' ')
        my_total.append(f'⏸{total_my_pd}', style='bold yellow')
    total_row = [
        Text('Total', style=f'bold {uc} {bg}'),
        Text('', style=bg),
        Text(total_emoji if total_emoji else '', style=bg),
        _bar(total_U, total_T, 14),
        tcnt,
        Text('', style=bg),
    ]
    if has_any_trend:
        total_row.append(Text('', style=bg))
    total_row.append(Text('│', style='bright_black'))
    total_row.append(my_total)
    table.add_row(*total_row)

    subtitle = f'[dim]{user}[/dim]'
    content = table

    return Panel(
        content,
        title='[bold]speek-[/bold] [dim]v0.0.3[/dim]',
        subtitle=subtitle,
        border_style='bright_blue',
        padding=(0, 1),
    )


def _fetch_my_gpus(user: str) -> Dict[str, Dict[str, int]]:
    """Return {partition: {R: gpu_count, PD: gpu_count}} for the user."""
    out = _run(['squeue', '-u', user, '-o', '%T|%P|%b', '-h'])
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {'R': 0, 'PD': 0})
    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + ['', '', ''])[:3]
        state, part, gres = parts[0].strip().upper(), parts[1].strip(), parts[2].strip()
        if not part:
            continue
        m = _FLEX_RE.search(gres or '')
        gpus = int(m.group(1)) if m else 1
        if state == 'RUNNING':
            counts[part]['R'] += gpus
        elif state == 'PENDING':
            counts[part]['PD'] += gpus
    return dict(counts)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    console = Console()
    w, h = console.size
    user = args.user or _run(['whoami']).strip()
    stats = _fetch_cluster()
    my_gpus = _fetch_my_gpus(user)
    panel = build_panel(stats, my_gpus, term_w=w, term_h=h, user=user)
    console.print()
    console.print(Align(panel, align='center'))
    console.print()


if __name__ == '__main__':
    main()
