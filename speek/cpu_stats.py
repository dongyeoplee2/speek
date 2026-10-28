"""cpu_stats.py — CPU-only partition stats shared by speek0, speek- and speek+ (--cpu)."""
from __future__ import annotations

import re
import subprocess
from collections import defaultdict
from typing import Dict

_DOWN_STATES = ('DOWN', 'DRAIN', 'DRAINING', 'DRAINED')


def _run(cmd: list) -> str:
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return ''


def fetch_cpu_stats() -> Dict[str, Dict]:
    """Return {partition: {Total, Used, Free, Nodes, DownCPUs, DownNodes, RAMperCPU}}
    for nodes without GPUs, counted in cores. Down/drained nodes count as fully used."""
    out = _run(['scontrol', 'show', 'node', '--oneliner'])
    agg: Dict[str, Dict] = defaultdict(lambda: {
        'Total': 0, 'Used': 0, 'Nodes': [], '_mem': 0,
        'DownCPUs': 0, 'DownNodes': 0,
    })
    for ln in out.splitlines():
        if not ln.strip():
            continue

        def _f(key: str) -> str:
            m = re.search(rf'\b{key}=(\S+)', ln)
            return m.group(1) if m else ''

        if re.search(r'gres/gpu=\d', _f('CfgTRES')):
            continue
        part = _f('Partitions')
        if not part or part == '(null)':
            continue
        try:
            total = int(_f('CPUTot') or 0)
            used = int(_f('CPUAlloc') or 0)
            mem = int(_f('RealMemory') or 0)
        except ValueError:
            continue
        if total == 0:
            continue
        state = _f('State').split('+')[0].rstrip('*~#$').upper()
        d = agg[part]
        d['Total'] += total
        d['_mem'] += mem
        if state in _DOWN_STATES:
            d['Used'] += total
            d['DownCPUs'] += total
            d['DownNodes'] += 1
        else:
            d['Used'] += min(used, total)
        d['Nodes'].append((_f('NodeName'), state))

    result: Dict[str, Dict] = {}
    for p, d in agg.items():
        result[p] = {
            'Total': d['Total'],
            'Used': d['Used'],
            'Free': d['Total'] - d['Used'],
            'Nodes': sorted(d['Nodes'], key=lambda x: x[0]),
            'DownCPUs': d['DownCPUs'],
            'DownNodes': d['DownNodes'],
            'RAMperCPU': d['_mem'] // d['Total'] // 1024 if d['_mem'] else None,  # GB
        }
    return result


def fetch_cpu_jobs(partitions) -> Dict[str, Dict]:
    """Return {partition: {'pending_jobs': N, 'users': {user: {'R': cores, 'PD': cores}}}}
    for running/pending jobs in the given partitions."""
    partitions = set(partitions)
    out = _run(['squeue', '-h', '-t', 'R,PD', '-o', '%u|%T|%P|%C'])
    result: Dict[str, Dict] = {p: {'pending_jobs': 0, 'users': {}} for p in partitions}
    for ln in out.splitlines():
        parts = ln.split('|')
        if len(parts) != 4:
            continue
        user, state, part, cpus = (s.strip() for s in parts)
        part = part.split(',')[0]
        if part not in partitions:
            continue
        try:
            cpus = int(cpus)
        except ValueError:
            continue
        key = 'R' if state == 'RUNNING' else 'PD'
        u = result[part]['users'].setdefault(user, {'R': 0, 'PD': 0})
        u[key] += cpus
        if key == 'PD':
            result[part]['pending_jobs'] += 1
    return result
