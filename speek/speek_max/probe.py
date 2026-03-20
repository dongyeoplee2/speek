"""probe.py — One-time SLURM cluster capability probe with local caching.

Run once on first launch (or when cache is stale/missing) to discover what the
cluster actually supports.  Results are cached so subsequent startups are instant.

Works on any SLURM installation: field availability, history depth, command
access, and scheduler configuration all vary between sites.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from typing import Dict, List, Optional

_PROBE_CACHE = os.path.expanduser('~/.config/speek/system_probe.json')
_PROBE_TTL   = 24 * 3600  # seconds — re-probe once per day

# All sacct fields speek-max would like to use.  The probe checks which subset
# actually exists on this cluster and saves the intersection.
DESIRED_SACCT_FIELDS: List[str] = [
    # Job identity
    'JobID', 'JobName', 'User', 'Group', 'Account', 'Partition',
    # Status
    'State', 'ExitCode', 'DerivedExitCode', 'FailedNode',
    # Time
    'Elapsed', 'Submit', 'Start', 'End', 'Timelimit',
    # Resources (metadata row)
    'AllocCPUS', 'AllocNodes', 'AllocTRES', 'ReqCPUS', 'ReqMem', 'ReqTRES',
    'NodeList', 'WorkDir',
    # Log path discovery — absent on some clusters
    'StdOut', 'StdErr', 'SubmitLine',
    # Misc
    'Priority', 'QOS', 'Comment',
    # Resource usage (batch step row)
    'MaxRSS', 'MaxVMSize', 'MaxDiskRead', 'MaxDiskWrite',
    'CPUTime', 'TotalCPU', 'UserCPU', 'SystemCPU', 'AveRSS', 'NTasks',
]

# Keys that must be present for a cached result to be considered complete.
_REQUIRED_KEYS = {'commands', 'cluster', 'sacct_fields', 'sacct_history'}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run(cmd: List[str], timeout: int = 8) -> Optional[str]:
    try:
        return subprocess.check_output(
            cmd, text=True, stderr=subprocess.DEVNULL, timeout=timeout,
        )
    except Exception:
        return None


def _timed_run(cmd: List[str], timeout: int = 8) -> tuple:
    """Run command and return (output_or_None, elapsed_ms)."""
    t = time.time()
    out = _run(cmd, timeout)
    return out, round((time.time() - t) * 1000)


# ── Individual probes ─────────────────────────────────────────────────────────

def _probe_commands() -> Dict[str, object]:
    """Check which SLURM CLI commands are accessible and record response times."""
    import getpass
    user = getpass.getuser()

    # Each entry: (name, availability_cmd, timing_cmd)
    # availability_cmd must exit 0; timing_cmd is what normal speek-max usage looks like
    checks = {
        'squeue':   (['squeue',   '--version'],
                     ['squeue',   '-h', '-o', '%T|%i', '--me']),
        'scontrol': (['scontrol', 'show', 'config'],
                     ['scontrol', 'show', 'node', '--oneliner']),
        'sacct':    (['sacct',    '--helpformat'],
                     ['sacct',    '-S', 'today', '-u', user,
                      '--format=JobID', '--parsable2', '--noheader', '--allocations']),
        'sinfo':    (['sinfo',    '--version'],
                     ['sinfo',    '-h', '-o', '%P|%a|%l']),
        'sprio':    (['sprio',    '--version'],
                     ['sprio',    '-o', '%i|%Y', '-h']),
        'sshare':   (['sshare',   '--version'],
                     ['sshare',   '-al', '-o', 'User,FairShare', '--noheader']),
        'scancel':  (['scancel',  '--version'],
                     ['scancel',  '--version']),
    }
    result: Dict[str, object] = {}
    for name, (avail_cmd, timing_cmd) in checks.items():
        out = _run(avail_cmd)
        available = out is not None
        _, ms = _timed_run(timing_cmd) if available else (None, 0)
        result[name] = {'ok': available, 'ms': ms}
    return result


def _probe_cluster_info() -> Dict[str, str]:
    """Parse SLURM version, cluster name, and scheduler from scontrol show config."""
    out = _run(['scontrol', 'show', 'config'])
    if not out:
        return {}
    patterns = {
        'slurm_version': r'SLURM_VERSION\s*=\s*(\S+)',
        'cluster_name':  r'ClusterName\s*=\s*(\S+)',
        'scheduler':     r'SchedulerType\s*=\s*(\S+)',
        'select_type':   r'SelectType\s*=\s*(\S+)',
        'priority_type': r'PriorityType\s*=\s*(\S+)',
        'max_job_count': r'MaxJobCount\s*=\s*(\S+)',
    }
    result: Dict[str, str] = {}
    for line in out.splitlines():
        for key, pat in patterns.items():
            if key not in result:
                m = re.search(pat, line)
                if m:
                    result[key] = m.group(1)
    return result


def _probe_sacct_fields() -> Dict[str, object]:
    """Run sacct --helpformat and determine which desired fields exist."""
    out = _run(['sacct', '--helpformat'])
    if not out:
        return {
            'available':         [],
            'desired_available': [],
            'desired_missing':   list(DESIRED_SACCT_FIELDS),
        }
    # sacct --helpformat prints field names separated by whitespace/newlines
    available: List[str] = re.findall(r'[A-Za-z][A-Za-z0-9]+', out)
    available_set = set(available)
    return {
        'available':         sorted(available_set),
        'desired_available': [f for f in DESIRED_SACCT_FIELDS if f     in available_set],
        'desired_missing':   [f for f in DESIRED_SACCT_FIELDS if f not in available_set],
    }


def _probe_sacct_history() -> Dict[str, object]:
    """Check whether sacct accepts -S 1970-01-01 (extended history access).
    Uses -u $USER --allocations to avoid scanning the entire cluster history.
    """
    import getpass
    user = getpass.getuser()
    _, ms = _timed_run(
        ['sacct', '-S', '1970-01-01', '-u', user, '--format=JobID',
         '--parsable2', '--noheader', '--allocations'],
        timeout=20,
    )
    out = _run(
        ['sacct', '-S', '1970-01-01', '-u', user, '--format=JobID',
         '--parsable2', '--noheader', '--allocations'],
        timeout=20,
    )
    return {'ok': out is not None, 'ms': ms}


# ── Aggregation ───────────────────────────────────────────────────────────────

def run_probes() -> dict:
    """Run all SLURM capability probes.  May take a few seconds on first run."""
    results: dict = {'timestamp': time.time()}
    results['commands']      = _probe_commands()
    results['cluster']       = _probe_cluster_info()
    results['sacct_fields']  = _probe_sacct_fields()
    results['sacct_history'] = _probe_sacct_history()

    # Derive log-path resolution strategy from discovered fields
    avail: List[str] = results['sacct_fields'].get('desired_available', [])
    if 'StdOut' in avail:
        strategy = 'sacct_stdout'         # direct sacct StdOut field
    elif 'SubmitLine' in avail:
        strategy = 'submit_line_parse'    # parse --output from sbatch command
    else:
        strategy = 'filesystem_fallback'  # guess from WorkDir patterns
    results['log_path_strategy'] = strategy

    return results


# ── Cache I/O ────────────────────────────────────────────────────────────────

def _load_cache() -> Optional[dict]:
    try:
        with open(_PROBE_CACHE) as f:
            return json.load(f)
    except Exception:
        return None


def _save_cache(d: dict) -> None:
    os.makedirs(os.path.dirname(_PROBE_CACHE), exist_ok=True)
    with open(_PROBE_CACHE, 'w') as f:
        json.dump(d, f, indent=2)


# ── Public API ────────────────────────────────────────────────────────────────

def load_cached_probe() -> Optional[dict]:
    """Return cached probe results if they exist and are fresh.  Never runs probes.
    Use this in hot paths (e.g. slurm.py field init) to avoid blocking."""
    cache = _load_cache()
    if not cache:
        return None
    age = time.time() - cache.get('timestamp', 0)
    if age > _PROBE_TTL:
        return None
    if not _REQUIRED_KEYS.issubset(cache.keys()):
        return None  # cache is from an older version — missing fields
    return cache


def get_probe_results(force: bool = False) -> dict:
    """Return probe results, running probes if cache is absent/stale/incomplete."""
    if not force:
        cached = load_cached_probe()
        if cached:
            return cached
    results = run_probes()
    _save_cache(results)
    return results


def cache_age_str() -> str:
    """Human-readable age of the current cache, or 'no cache'."""
    cache = _load_cache()
    if not cache:
        return 'no cache'
    age = time.time() - cache.get('timestamp', 0)
    if age < 60:
        return 'just now'
    if age < 3600:
        return f'{int(age / 60)}m ago'
    if age < 86400:
        return f'{int(age / 3600)}h ago'
    return f'{int(age / 86400)}d ago'
