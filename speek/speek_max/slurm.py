"""slurm.py — SLURM data layer for speek-max-2. All stdlib only."""
from __future__ import annotations

import re
import subprocess
import threading as _threading
import time as _time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor, as_completed as _as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# ── Regex constants ────────────────────────────────────────────────────────────

GPU_RE_NAMED = re.compile(r'gpu:([A-Za-z0-9\-]+)[:=](\d+)', re.IGNORECASE)
FLEX_RE = re.compile(r'gpu(?::[A-Za-z0-9_\-]+)?[:=](\d+)', re.IGNORECASE)
GPU_RE = re.compile(r'gpu(?::[A-Za-z0-9_\-]+)?[:=](\d+)', re.IGNORECASE)

_LOG_RE   = re.compile(r'StdOut=(\S+)')
_DT_FMT   = '%Y-%m-%dT%H:%M:%S'

# ── scontrol TTL cache ─────────────────────────────────────────────────────────

_scontrol_cache: Dict[str, Tuple[float, str]] = {}
_SCONTROL_TTL = 15.0  # seconds


def _scontrol_show_job(job_id: str) -> Optional[str]:
    """Run `scontrol show job <id>` with a 15-second TTL cache."""
    now = _time.monotonic()
    cached = _scontrol_cache.get(job_id)
    if cached:
        ts, raw = cached
        if now - ts < _SCONTROL_TTL:
            return raw
    try:
        raw = subprocess.check_output(
            ['scontrol', 'show', 'job', job_id],
            text=True, stderr=subprocess.DEVNULL,
        )
        _scontrol_cache[job_id] = (now, raw)
        return raw
    except Exception:
        _scontrol_cache.pop(job_id, None)
        return None

# ── squeue / sprio TTL caches ──────────────────────────────────────────────────
# Multiple widgets call squeue independently within the same 5s window.
# These caches collapse N subprocess calls into 1 per TTL period.

_SQUEUE_TTL = 5.0   # seconds — matches the default _queue_refresh interval
_CLUSTER_TTL = 10.0 # seconds — matches ClusterBar interval; scontrol is expensive

_queue_cache:         Optional[Tuple[float, List]]  = None
_job_stats_cache:     Optional[Tuple[float, Dict]]  = None
_priorities_cache:    Optional[Tuple[float, Dict]]  = None
_my_jobs_cache:       Dict[str, Tuple[float, List]] = {}
_cluster_stats_cache: Optional[Tuple[float, Dict]]  = None

_queue_lock        = _threading.Lock()
_job_stats_lock    = _threading.Lock()
_priorities_lock   = _threading.Lock()
_my_jobs_lock      = _threading.Lock()
_cluster_stats_lock = _threading.Lock()


# ── Model aliases ──────────────────────────────────────────────────────────────

_MODEL_ALIASES: Dict[str, str] = {
    'A100-SXM4-80GB': 'A100-80GB',
    'A100-SXM4-40GB': 'A100-40GB',
    'A100-PCIE-40GB': 'A100-40GB',
}

# VRAM lookup by normalised model name (GB)
_MODEL_VRAM: Dict[str, int] = {
    'H200':      141,
    'H100':       80,
    'A100':       80,
    'A100-80GB':  80,
    'A100-40GB':  40,
    '4A100':      40,
    'L40S':       48,
    'L40':        48,
    'A6000':      48,
    'PRO6000':    48,
    'A5000':      24,
    '3090':       24,
    '4090':       24,
    '2080ti':     11,
    'V100-32GB':  32,
    'V100':       16,
    'T4':         16,
}


# ── Cluster stats ──────────────────────────────────────────────────────────────

def _norm(m: str) -> str:
    m = (m or '').strip()
    for prefix, alias in _MODEL_ALIASES.items():
        if m.startswith(prefix):
            return alias
    return m


def _parse_count(s: str) -> int:
    s = re.sub(r'\(IDX:[^)]*\)', '', s or '')
    mt = FLEX_RE.search(s)
    return int(mt.group(1)) if mt else 0


def fetch_cluster_stats() -> Dict[str, Dict]:
    """Returns {model: {Total, Used, Free, VRAM, Nodes}} from scontrol show node.
    Results are cached for _CLUSTER_TTL seconds."""
    global _cluster_stats_cache
    now = _time.monotonic()
    with _cluster_stats_lock:
        if _cluster_stats_cache is not None and now - _cluster_stats_cache[0] < _CLUSTER_TTL:
            return _cluster_stats_cache[1]
    try:
        out = subprocess.check_output(
            ['scontrol', 'show', 'node', '--oneliner'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return {}

    agg: Dict[str, Dict] = defaultdict(lambda: {
        'Total': 0, 'Used': 0, 'Nodes': [],
        '_cpu_total': 0, '_mem_total': 0, '_node_count': 0,
    })

    for ln in out.splitlines():
        if not ln.strip():
            continue

        def _field(key: str) -> str:
            mt = re.search(rf'{key}=(\S+)', ln)
            return mt.group(1) if mt else ''

        node      = _field('NodeName')
        gres_fld  = _field('Gres')
        cfg_tres  = _field('CfgTRES')
        alloc_tres = _field('AllocTRES')
        gres_used = _field('GresUsed')

        mm = _NODE_GPU_RE.search(gres_fld)
        model = _norm(mm.group(1)) if mm else None

        # total: CfgTRES → Gres field count → flexible match
        mg = re.search(r'gres/gpu=(\d+)', cfg_tres)
        if mg:
            total = int(mg.group(1))
        elif mm:
            total = int(mm.group(2))
        else:
            flex = FLEX_RE.search(gres_fld)
            total = int(flex.group(1)) if flex else 0

        if total == 0:
            continue

        if model is None:
            model = 'GPU'

        # used: AllocTRES → GresUsed field
        mu = re.search(r'gres/gpu=(\d+)', alloc_tres)
        if mu:
            used = int(mu.group(1))
        else:
            gu = re.search(r'gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', gres_used, re.IGNORECASE)
            used = int(gu.group(1)) if gu else 0

        state = _field('State').split('+')[0].rstrip('*~#$').upper()

        # CPU and memory per node
        cpu_total = 0
        mem_mb = 0
        try:
            cpu_total = int(_field('CPUTot') or 0)
        except ValueError:
            pass
        try:
            mem_mb = int(_field('RealMemory') or 0)
        except ValueError:
            pass

        used = min(used, total)
        agg[model]['Total'] += total
        agg[model]['Used']  += used
        agg[model]['Nodes'].append((node, state))
        agg[model]['_cpu_total'] += cpu_total
        agg[model]['_mem_total'] += mem_mb
        agg[model]['_node_count'] += 1

    result: Dict[str, Dict] = {}
    for m, d in agg.items():
        vram = _MODEL_VRAM.get(m)
        if vram is None:
            mv = re.search(r'(\d+)GB', m, re.IGNORECASE)
            if mv:
                vram = int(mv.group(1))
        n_gpus = d['Total'] or 1
        cpu_per_gpu = d['_cpu_total'] // n_gpus if d['_cpu_total'] else None
        ram_per_gpu = d['_mem_total'] // n_gpus // 1024 if d['_mem_total'] else None  # GB
        result[m] = {
            'Total': d['Total'],
            'Used':  d['Used'],
            'Free':  d['Total'] - d['Used'],
            'VRAM':  vram,
            'CPUperGPU': cpu_per_gpu,
            'RAMperGPU': ram_per_gpu,
            'Nodes': sorted(d['Nodes'], key=lambda x: x[0]),
        }
    with _cluster_stats_lock:
        _cluster_stats_cache = (_time.monotonic(), result)
    return result


# ── Priority / fairshare ───────────────────────────────────────────────────────

def fetch_all_priorities() -> Dict[str, Dict]:
    """Return {job_id: {total, age, fairshare, jobsize, qos, partition, user}}
    for all pending jobs via sprio.  Results are cached for _SQUEUE_TTL seconds."""
    global _priorities_cache
    now = _time.monotonic()
    with _priorities_lock:
        if _priorities_cache is not None and now - _priorities_cache[0] < _SQUEUE_TTL:
            return _priorities_cache[1]
    try:
        out = subprocess.check_output(
            ['sprio', '-o', '%i|%u|%Y|%a|%f|%j|%q|%p', '-h'],
            text=True, stderr=subprocess.DEVNULL, timeout=8,
        )
    except Exception:
        return {}
    result = {}
    for ln in out.splitlines():
        parts = (ln.strip().split('|') + [''] * 8)[:8]
        jid, user, total, age, fs, jobsize, qos, part = parts
        try:
            result[jid.strip()] = {
                'user':      user.strip(),
                'total':     int(total.strip()),
                'age':       float(age.strip()),
                'fairshare': float(fs.strip()),
                'jobsize':   float(jobsize.strip()),
                'qos':       float(qos.strip()),
                'partition': part.strip(),
            }
        except (ValueError, AttributeError):
            pass
    with _priorities_lock:
        _priorities_cache = (_time.monotonic(), result)
    return result


def fetch_fairshares() -> Dict[str, float]:
    """Return {username: fairshare_score} from sshare."""
    try:
        out = subprocess.check_output(
            ['sshare', '-al', '-o', 'User,FairShare', '--noheader'],
            text=True, stderr=subprocess.DEVNULL, timeout=8,
        )
    except Exception:
        return {}
    result = {}
    for ln in out.splitlines():
        parts = ln.split()
        if len(parts) >= 2:
            try:
                result[parts[0].strip()] = float(parts[1].strip())
            except ValueError:
                pass
    return result


# ── Job statistics ─────────────────────────────────────────────────────────────

def fetch_job_stats() -> Dict:
    """Returns cluster-wide job statistics.

    Returns a dict with keys ``by_model``, ``total_R``, ``total_PD``, and ``active_users``.
    Results are cached for _SQUEUE_TTL seconds.
    """
    global _job_stats_cache
    now = _time.monotonic()
    with _job_stats_lock:
        if _job_stats_cache is not None and now - _job_stats_cache[0] < _SQUEUE_TTL:
            return _job_stats_cache[1]
    try:
        out = subprocess.check_output(
            ['squeue', '-o', '%T|%u|%P|%b', '-h',
             '--states=RUNNING,PENDING'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return {'by_model': {}, 'total_R': 0, 'total_PD': 0, 'active_users': 0}

    by_model: Dict[str, Dict[str, int]] = defaultdict(lambda: {'R': 0, 'PD': 0})
    users: set = set()
    total_R = total_PD = 0

    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + ['', '', '', ''])[:4]
        state, user, partition, gres = (
            parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()
        )

        mt = GPU_RE_NAMED.search(gres)
        if mt:
            model = _norm(mt.group(1))
        else:
            flex = FLEX_RE.search(gres)
            # Use partition name as model fallback
            model = partition if (flex and partition) else ('GPU' if flex else None)

        if not model:
            continue

        users.add(user)
        if state == 'RUNNING':
            by_model[model]['R'] += 1
            total_R += 1
        elif state == 'PENDING':
            by_model[model]['PD'] += 1
            total_PD += 1

    # per-user running GPU count
    user_gpus: Dict[str, int] = defaultdict(int)
    try:
        uout = subprocess.check_output(
            ['squeue', '-t', 'R', '-o', '%u|%b', '-h'],
            text=True, stderr=subprocess.DEVNULL,
        )
        for ln in uout.splitlines():
            if not ln.strip():
                continue
            u, gres = (ln.split('|') + ['', ''])[:2]
            mt = GPU_RE_NAMED.search(gres.strip())
            if mt:
                try:
                    user_gpus[u.strip()] += int(mt.group(2))
                except ValueError:
                    pass
            else:
                flex = FLEX_RE.search(gres.strip())
                if flex:
                    try:
                        user_gpus[u.strip()] += int(flex.group(1))
                    except ValueError:
                        pass
    except Exception:
        pass

    result = {
        'by_model': dict(by_model),
        'total_R': total_R,
        'total_PD': total_PD,
        'active_users': len(users),
        'user_gpus': dict(user_gpus),
    }
    with _job_stats_lock:
        _job_stats_cache = (_time.monotonic(), result)
    return result


# ── Queue ──────────────────────────────────────────────────────────────────────

def _gpu_count(gres: str) -> str:
    mt = GPU_RE.search(gres or '')
    return mt.group(1) if mt else '-'


def fetch_queue() -> List[Tuple]:
    """Returns list of row tuples: (jobid, user, name, partition, gpus, state, elapsed).
    Results are cached for _SQUEUE_TTL seconds."""
    global _queue_cache
    now = _time.monotonic()
    with _queue_lock:
        if _queue_cache is not None and now - _queue_cache[0] < _SQUEUE_TTL:
            return _queue_cache[1]
    try:
        out = subprocess.check_output(
            ['squeue', '-o', '%i|%u|%j|%P|%b|%T|%M', '-h',
             '--states=RUNNING,PENDING'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []
    rows = []
    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + [''] * 7)[:7]
        jid, user, name, part, gres, state, elapsed = parts
        rows.append((
            jid.strip(),
            user.strip(),
            name.strip(),
            part.strip(),
            _gpu_count(gres.strip()),
            state.strip(),
            elapsed.strip(),
        ))
    rows.sort(key=lambda r: (0 if r[5] == 'RUNNING' else 1, r[1]))
    with _queue_lock:
        _queue_cache = (_time.monotonic(), rows)
    return rows


# ── My jobs ───────────────────────────────────────────────────────────────────

def _fmt_eta(s: str) -> str:
    """Format squeue %S (scheduled start time) as 'in Xh Ym' or raw."""
    if not s or s in ('N/A', 'Unknown'):
        return '?'
    try:
        dt = datetime.strptime(s.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S')
        delta = dt - datetime.now()
        secs = int(delta.total_seconds())
        if secs < 0:
            return 'soon'
        h, r = divmod(secs, 3600)
        m, _ = divmod(r, 60)
        return f'~{h}h {m:02d}m' if h else f'~{m}m'
    except Exception:
        return s


def fetch_my_jobs(user: str) -> List[Tuple]:
    """Returns [(jid, name, partition, gpus, state, elapsed, eta), ...].
    Results are cached per-user for _SQUEUE_TTL seconds."""
    now = _time.monotonic()
    with _my_jobs_lock:
        entry = _my_jobs_cache.get(user)
        if entry is not None and now - entry[0] < _SQUEUE_TTL:
            return entry[1]
    try:
        out = subprocess.check_output(
            ['squeue', '-u', user, '-o', '%i|%j|%P|%b|%T|%M|%S', '-h'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []
    rows = []
    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + [''] * 7)[:7]
        jid, name, part, gres, state, elapsed, eta_raw = parts
        eta = _fmt_eta(eta_raw) if state.strip().upper() == 'PENDING' else ''
        rows.append((
            jid.strip(),
            name.strip(),
            part.strip(),
            _gpu_count(gres.strip()),
            state.strip(),
            elapsed.strip(),
            eta,
        ))
    rows.sort(key=lambda r: (0 if r[4] == 'RUNNING' else 1))
    with _my_jobs_lock:
        _my_jobs_cache[user] = (_time.monotonic(), rows)
    return rows


def fetch_job_details(job_id: str) -> Dict[str, str]:
    """Job details dict — scontrol for active jobs, sacct for finished jobs."""
    raw = _scontrol_show_job(job_id)
    if raw:
        result: Dict[str, str] = {}
        for token in re.split(r'\s+', raw):
            if '=' in token:
                k, _, v = token.partition('=')
                result[k.strip()] = v.strip()
        return result
    return _sacct_job_details(job_id)


def _sacct_log_path(job_id: str) -> Optional[str]:
    """Resolve stdout log path for completed/failed/cancelled jobs.

    Adapts to what the cluster supports (discovered via probe):
    - If StdOut sacct field is available: use it directly
    - Else if SubmitLine available: parse --output from sbatch command
    - Then: scan #SBATCH --output in the script file
    - Finally: filesystem pattern scan ({WorkDir}/out/{jobid}.out etc.)
    """
    import os as _os
    import shlex as _shlex

    _ensure_sacct_fields()
    avail_set = set(_SACCT_DETAIL_FIELDS)
    has_stdout = 'StdOut' in avail_set
    has_submitline = 'SubmitLine' in avail_set

    # Build minimal query for log-path resolution
    query_fields = ['JobID', 'JobName', 'User', 'WorkDir']
    if has_stdout:
        query_fields.append('StdOut')
    if has_submitline:
        query_fields.append('SubmitLine')

    work_dir = ''
    job_name = ''
    user     = ''
    submit_line = ''
    stdout_path = ''

    try:
        out = subprocess.check_output(
            ['sacct', '-j', job_id,
             f'--format={",".join(query_fields)}',
             '--parsable2', '--noheader', '-S', '1970-01-01'],
            text=True, stderr=subprocess.DEVNULL,
        )
        for line in out.splitlines():
            parts = line.strip().split('|')
            jid = parts[0].strip() if parts else ''
            if '.' in jid or not jid:
                continue
            job_name = parts[1].strip() if len(parts) > 1 else ''
            user     = parts[2].strip() if len(parts) > 2 else ''
            work_dir = parts[3].strip() if len(parts) > 3 else ''
            idx = 4
            if has_stdout:
                stdout_path = parts[idx].strip() if len(parts) > idx else ''
                idx += 1
            if has_submitline:
                submit_line = parts[idx].strip() if len(parts) > idx else ''
            break
    except Exception:
        pass

    # 0. Direct StdOut field (if available on this cluster)
    if stdout_path and stdout_path not in ('', 'Unknown', 'None', 'none'):
        return _expand_log_path(stdout_path, job_id, job_name, user)

    # 1. --output flag in the sbatch command line
    if submit_line:
        try:
            tokens = _shlex.split(submit_line)
            for i, tok in enumerate(tokens):
                if tok in ('-o', '--output') and i + 1 < len(tokens):
                    return _expand_log_path(tokens[i + 1], job_id, job_name, user)
                if tok.startswith('--output='):
                    return _expand_log_path(tok.split('=', 1)[1], job_id, job_name, user)
        except Exception:
            pass

    # 2. #SBATCH --output in the script file (last positional arg in SubmitLine)
    if submit_line:
        try:
            tokens = _shlex.split(submit_line)
            script = tokens[-1] if tokens else ''
            if script and not script.startswith('-'):
                script_path = script if _os.path.isabs(script) else (
                    _os.path.join(work_dir, script) if work_dir else script
                )
                with open(script_path) as sf:
                    for ln in sf:
                        ln = ln.strip()
                        if not ln.startswith('#SBATCH'):
                            continue
                        m = re.match(r'#SBATCH\s+(?:-o|--output)[=\s]+(\S+)', ln)
                        if m:
                            return _expand_log_path(m.group(1), job_id, job_name, user)
        except Exception:
            pass

    # 3. Filesystem fallback patterns
    if work_dir:
        candidates = [
            _os.path.join(work_dir, 'out', f'{job_id}.out'),
            _os.path.join(work_dir, f'slurm-{job_id}.out'),
            _os.path.join(work_dir, f'{job_id}.out'),
        ]
        for candidate in candidates:
            if _os.path.exists(candidate):
                return candidate

    return None


# Full wishlist — all fields speek-max would like to use.
# The probe filters this to what the cluster actually supports.
_SACCT_DESIRED_DETAIL: List[str] = [
    'JobID', 'JobName', 'User', 'Group', 'Account', 'Partition',
    'State', 'ExitCode', 'DerivedExitCode', 'FailedNode',
    'Elapsed', 'Submit', 'Start', 'End', 'Timelimit',
    'AllocCPUS', 'AllocNodes', 'AllocTRES', 'ReqCPUS', 'ReqMem', 'ReqTRES',
    'NodeList', 'WorkDir',
    'StdOut', 'StdErr', 'SubmitLine',   # vary by SLURM version
    'Priority', 'QOS', 'Comment',
]

# Fields collected from the .batch step row (resource usage — only present there)
_SACCT_BATCH_FIELDS: List[str] = [
    'MaxRSS', 'MaxVMSize', 'MaxDiskRead', 'MaxDiskWrite',
    'CPUTime', 'TotalCPU', 'UserCPU', 'SystemCPU', 'AveRSS', 'NTasks',
]

# Active field lists — initialised from probe cache on first use; fall back to
# a safe subset (excludes StdOut/StdErr which are absent on SLURM ≥ 23.11).
_SACCT_DETAIL_FIELDS: List[str] = [
    f for f in _SACCT_DESIRED_DETAIL
    if f not in ('StdOut', 'StdErr')   # safe default: omit fields known to vary
]

_SACCT_ALL_FIELDS: List[str] = _SACCT_DETAIL_FIELDS + [
    f for f in _SACCT_BATCH_FIELDS if f not in _SACCT_DETAIL_FIELDS
]

# Thread-safety for field list initialisation
_sacct_fields_lock  = _threading.Lock()
_sacct_fields_ready = False


def _ensure_sacct_fields() -> None:
    """Filter _SACCT_DETAIL_FIELDS to what the probe says is available.
    Reads only the JSON cache — never runs probes — so it is always fast.
    """
    global _sacct_fields_ready, _SACCT_DETAIL_FIELDS, _SACCT_ALL_FIELDS
    if _sacct_fields_ready:
        return
    with _sacct_fields_lock:
        if _sacct_fields_ready:
            return
        try:
            from speek.speek_max.probe import load_cached_probe
            probe = load_cached_probe()
            if probe:
                avail = set(probe.get('sacct_fields', {}).get('available', []))
                if avail:
                    _SACCT_DETAIL_FIELDS = [
                        f for f in _SACCT_DESIRED_DETAIL if f in avail
                    ]
                    _SACCT_ALL_FIELDS = _SACCT_DETAIL_FIELDS + [
                        f for f in _SACCT_BATCH_FIELDS
                        if f not in _SACCT_DETAIL_FIELDS
                    ]
        except Exception:
            pass
        _sacct_fields_ready = True


def apply_probe(probe: dict) -> None:
    """Update sacct field lists from freshly-collected probe results.
    Called by sysinfo_widget after a manual re-probe."""
    global _sacct_fields_ready, _SACCT_DETAIL_FIELDS, _SACCT_ALL_FIELDS
    with _sacct_fields_lock:
        try:
            avail = set(probe.get('sacct_fields', {}).get('available', []))
            if avail:
                _SACCT_DETAIL_FIELDS = [
                    f for f in _SACCT_DESIRED_DETAIL if f in avail
                ]
                _SACCT_ALL_FIELDS = _SACCT_DETAIL_FIELDS + [
                    f for f in _SACCT_BATCH_FIELDS
                    if f not in _SACCT_DETAIL_FIELDS
                ]
        except Exception:
            pass
        _sacct_fields_ready = True


# Map sacct field names → scontrol-compatible keys expected by _build_table / _title_from_details
_SACCT_TO_SCONTROL: Dict[str, str] = {
    'JobID':     'JobId',
    'User':      'UserId',
    'Group':     'GroupId',
    'State':     'JobState',
    'AllocNodes': 'NumNodes',
    'AllocCPUS':  'NumCPUs',
    'AllocTRES':  'TRES',
    'Submit':    'SubmitTime',
    'Start':     'StartTime',
    'End':       'EndTime',
    'Timelimit': 'TimeLimit',
    'Comment':   'Reason',
}

_sacct_details_cache: Dict[str, Tuple[float, Dict[str, str]]] = {}
_SACCT_DETAILS_TTL = 300.0  # completed jobs don't change; 5-minute cache


def _fmt_kb(val: str) -> str:
    """Convert a sacct memory string like '4644596K' to a human-readable value."""
    if not val:
        return val
    val = val.strip()
    multiplier = 1
    if val.endswith('K'):
        multiplier, val = 1024, val[:-1]
    elif val.endswith('M'):
        multiplier, val = 1024 ** 2, val[:-1]
    elif val.endswith('G'):
        multiplier, val = 1024 ** 3, val[:-1]
    try:
        b = float(val) * multiplier
    except ValueError:
        return val + ('K' if multiplier == 1024 else '')
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if b < 1024:
            return f'{b:.1f} {unit}'
        b /= 1024
    return f'{b:.1f} PB'


_SACCT_FMT_MEM = {'MaxRSS', 'MaxVMSize', 'AveRSS', 'MaxDiskRead', 'MaxDiskWrite'}


def _sacct_parse_row(parts: List[str], fields: List[str],
                     remap: Dict[str, str]) -> Dict[str, str]:
    """Extract non-empty fields from one parsable2 sacct row into a dict."""
    out: Dict[str, str] = {}
    for i, field in enumerate(fields):
        val = parts[i].strip() if i < len(parts) else ''
        if val and val not in ('None', 'Unknown', ''):
            out[remap.get(field, field)] = val
    return out


def _sacct_merge_batch(result: Dict[str, str], batch: Dict[str, str]) -> None:
    """Merge batch-step resource stats into *result*, formatting memory/IO values."""
    for field in _SACCT_BATCH_FIELDS:
        val = batch.get(field, '')
        if val:
            result[field] = _fmt_kb(val) if field in _SACCT_FMT_MEM else val


def _sacct_job_details(job_id: str) -> Dict[str, str]:
    """Fetch job details via sacct for completed/failed/cancelled jobs.

    Collects metadata from the main job row and resource-usage stats from the
    .batch step row, then merges them into a single dict with scontrol-compatible
    keys so the existing _build_table / _title_from_details renderers work unchanged.
    """
    _ensure_sacct_fields()   # trim fields to what this cluster supports

    now = _time.monotonic()
    cached = _sacct_details_cache.get(job_id)
    if cached and now - cached[0] < _SACCT_DETAILS_TTL:
        return cached[1]

    try:
        raw = subprocess.check_output(
            ['sacct', '-j', job_id,
             f'--format={",".join(_SACCT_ALL_FIELDS)}',
             '--parsable2', '--noheader', '-S', '1970-01-01'],
            text=True, stderr=subprocess.DEVNULL, timeout=15,
        )
    except Exception:
        return {}

    result: Dict[str, str] = {}
    batch:  Dict[str, str] = {}
    batch_jid = f'{job_id}.batch'

    for line in raw.splitlines():
        parts = line.split('|')
        jid   = parts[0].strip() if parts else ''
        if jid == batch_jid or (not result and jid.endswith('.batch')):
            batch = _sacct_parse_row(parts, _SACCT_ALL_FIELDS, {})
        elif '.' not in jid and not result:
            result = _sacct_parse_row(parts, _SACCT_ALL_FIELDS, _SACCT_TO_SCONTROL)

    _sacct_merge_batch(result, batch)
    _sacct_details_cache[job_id] = (now, result)
    return result


def _expand_log_path(path: str, job_id: str, job_name: str = '', user: str = '') -> str:
    """Expand SLURM % substitutions in a log path."""
    import getpass
    path = path.replace('%j', job_id)
    path = path.replace('%u', user or getpass.getuser())
    path = path.replace('%x', job_name)
    if '_' in job_id:
        array_id, task_id = job_id.split('_', 1)
        path = path.replace('%A', array_id)
        path = path.replace('%a', task_id)
    else:
        path = path.replace('%A', job_id)
        path = path.replace('%a', '0')
    return path


def get_job_log_path(job_id: str) -> Optional[str]:
    """Get StdOut path for a job — tries scontrol (active jobs) then sacct (finished jobs)."""
    # 1. scontrol — works for running/pending jobs (cached)
    raw = _scontrol_show_job(job_id)
    if raw:
        mt = _LOG_RE.search(raw)
        if mt:
            name_m = re.search(r'JobName=(\S+)', raw)
            user_m = re.search(r'\bUserId=(\w+)', raw)
            job_name = name_m.group(1) if name_m else ''
            user     = user_m.group(1).split('(')[0] if user_m else ''
            return _expand_log_path(mt.group(1), job_id, job_name, user)
    # 2. sacct — works for completed/failed/cancelled jobs
    return _sacct_log_path(job_id)


def fetch_job_details_and_log_path(
    job_id: str, sacct_fallback: bool = True
) -> Tuple[Dict[str, str], Optional[str]]:
    """Details dict + stdout log path — scontrol for active jobs, sacct for finished jobs.

    When *sacct_fallback* is False the sacct queries are skipped (useful when
    sacct is disabled in Config or causes high latency on the cluster).
    """
    raw = _scontrol_show_job(job_id)
    details: Dict[str, str] = {}
    log_path: Optional[str] = None

    if raw:
        for token in re.split(r'\s+', raw):
            if '=' in token:
                k, _, v = token.partition('=')
                details[k.strip()] = v.strip()
        mt = _LOG_RE.search(raw)
        if mt:
            name_m = re.search(r'JobName=(\S+)', raw)
            user_m = re.search(r'\bUserId=(\w+)', raw)
            job_name = name_m.group(1) if name_m else ''
            user     = user_m.group(1).split('(')[0] if user_m else ''
            log_path = _expand_log_path(mt.group(1), job_id, job_name, user)

    if not details and sacct_fallback:
        # scontrol returned nothing → job is finished; fetch from sacct
        details = _sacct_job_details(job_id)

    if not log_path and sacct_fallback:
        log_path = _sacct_log_path(job_id)

    return details, log_path


# ── History ───────────────────────────────────────────────────────────────────

def fetch_history(days: int) -> List[Tuple]:
    """Returns sacct history tuples: (jid, name, partition, start, elapsed, state, exit_code, alloc_tres, nodelist).

    For PENDING jobs, 'start' is the submit time (since Start is 'Unknown').
    """
    start = (datetime.now() - timedelta(days=days)).strftime(_DT_FMT)
    try:
        out = subprocess.check_output(
            ['sacct', '-S', start, '--parsable2', '--noheader',
             '--format=JobID,JobName,Partition,Start,Elapsed,State,ExitCode,AllocTRES,NodeList,Submit'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []
    rows = []
    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + [''] * 10)[:10]
        jid, name, part, start_t, elapsed, state, exit_code, alloc_tres, nodelist, submit_t = parts
        if '.' in jid:
            continue
        # For PENDING jobs, Start is 'Unknown' — use Submit time instead
        effective_start = start_t.strip()
        if effective_start in ('Unknown', '', 'None'):
            effective_start = submit_t.strip()
        rows.append((
            jid.strip(),
            name.strip(),
            part.strip(),
            effective_start,
            elapsed.strip(),
            state.strip(),
            exit_code.strip(),
            alloc_tres.strip(),
            nodelist.strip(),
        ))
    return rows


# ── Nodes ─────────────────────────────────────────────────────────────────────

_NODE_GPU_RE = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)


def parse_nodes() -> List[Tuple]:
    """Returns [(node, partitions, model, free, total, state, reason), ...]."""
    try:
        out = subprocess.check_output(
            ['scontrol', 'show', 'node', '--oneliner'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []

    rows = []
    for ln in out.splitlines():
        if not ln.strip():
            continue

        def _field(key: str) -> str:
            mt = re.search(rf'{key}=(\S+)', ln)
            return mt.group(1) if mt else ''

        node = _field('NodeName')
        parts = _field('Partitions')
        state = _field('State').split('+')[0].lower()
        reason = _field('Reason').replace('(null)', '').strip()
        cfg_gres = _field('CfgTRES')

        gres_field = _field('Gres')
        mm = _NODE_GPU_RE.search(gres_field)
        model = _norm(mm.group(1)) if mm else '?'

        # Total: prefer CfgTRES gres/gpu, fall back to Gres field count
        mg = re.search(r'gres/gpu=(\d+)', cfg_gres)
        if mg:
            total_gpu = int(mg.group(1))
        elif mm:
            total_gpu = int(mm.group(2))
        else:
            flex = re.search(r'gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', gres_field, re.IGNORECASE)
            total_gpu = int(flex.group(1)) if flex else 0
        if total_gpu == 0:
            continue

        alloc_gres = _field('AllocTRES')
        mu = re.search(r'gres/gpu=(\d+)', alloc_gres)
        if mu:
            used_gpu = int(mu.group(1))
        else:
            # fall back to GresUsed field
            gres_used_field = _field('GresUsed')
            gu = re.search(r'gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', gres_used_field, re.IGNORECASE)
            used_gpu = int(gu.group(1)) if gu else 0
        free_gpu = max(total_gpu - used_gpu, 0)

        rows.append((node, parts, model, free_gpu, total_gpu, state, reason))

    rows.sort(key=lambda r: (r[1], -r[3], r[0]))
    return rows


# ── Priority ──────────────────────────────────────────────────────────────────

def _sprio(job_id: str) -> Dict[str, str]:
    """Parse sprio -j <jobid> output into {component: value}."""
    try:
        out = subprocess.check_output(
            ['sprio', '-j', job_id, '-o', '%i %Y %a %f %j %q %t'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return {}
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    if len(lines) < 2:
        return {}
    headers = lines[0].split()
    values = lines[1].split()
    return dict(zip(headers, values))


def _squeue_reason(job_id: str) -> str:
    try:
        out = subprocess.check_output(
            ['squeue', '-j', job_id, '-o', '%R', '-h'],
            text=True, stderr=subprocess.DEVNULL,
        )
        return out.strip()
    except Exception:
        return '?'


def _sshare_my(user: str) -> Optional[float]:
    """Return current user's fairshare score."""
    try:
        out = subprocess.check_output(
            ['sshare', '-U', '-u', user, '-o', 'User,FairShare', '--noheader'],
            text=True, stderr=subprocess.DEVNULL,
        )
        for ln in out.splitlines():
            parts = ln.split()
            if len(parts) >= 2 and parts[0].strip() == user:
                return float(parts[1])
    except Exception:
        pass
    return None


def _eta(job_id: str) -> Optional[str]:
    """Get scheduled start time from squeue %S."""
    try:
        out = subprocess.check_output(
            ['squeue', '-j', job_id, '-o', '%S', '-h'],
            text=True, stderr=subprocess.DEVNULL,
        )
        s = out.strip()
        if s and s not in ('N/A', 'Unknown'):
            return s
    except Exception:
        pass
    return None


def fetch_priority_data(job_id: str, user: str) -> Dict:
    prio = _sprio(job_id)
    reason = _squeue_reason(job_id)
    share = _sshare_my(user)
    eta = _eta(job_id)
    return {
        'job_id': job_id,
        'reason': reason,
        'prio': prio,
        'share': share,
        'eta': eta,
    }


# ── User statistics ───────────────────────────────────────────────────────────

def _parse_elapsed_seconds(s: str) -> int:
    """Parse SLURM elapsed like '2-03:14:55' or '03:14:55' into seconds."""
    s = (s or '').strip()
    days = 0
    if '-' in s:
        d, s = s.split('-', 1)
        try:
            days = int(d)
        except ValueError:
            pass
    parts = s.split(':')
    try:
        if len(parts) == 3:
            return days * 86400 + int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        if len(parts) == 2:
            return days * 86400 + int(parts[0]) * 60 + int(parts[1])
    except ValueError:
        pass
    return 0


def fetch_user_stats(days: int = 30) -> List[Dict]:
    """Per-user analysis from sacct + current squeue state.

    Returns list of dicts sorted by gpu_hours desc:
      user, running_gpus, pending_jobs, total_jobs, completed,
      failed, cancelled, gpu_hours, avg_secs, top_partition
    """
    start = (datetime.now() - timedelta(days=days)).strftime(_DT_FMT)

    # ── sacct history ──────────────────────────────────────────────────
    _sacct_base = ['sacct', '-S', start, '--parsable2', '--noheader',
                   '--format=User,State,Elapsed,AllocTRES,Partition']
    out = ''
    for _cmd in [_sacct_base + ['--allusers'], _sacct_base]:
        try:
            out = subprocess.check_output(
                _cmd, text=True, stderr=subprocess.DEVNULL, timeout=8,
            )
            break
        except Exception:
            continue

    hist: Dict[str, Dict] = defaultdict(lambda: {
        'total': 0, 'completed': 0, 'failed': 0, 'cancelled': 0,
        'gpu_secs': 0.0, 'elapsed_secs': 0, 'partitions': defaultdict(int),
    })

    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + [''] * 5)[:5]
        user, state, elapsed, alloc_gres, partition = parts
        user = user.strip()
        if not user or '.' in user:   # skip step lines (user.step)
            continue
        state = state.strip().upper()
        # skip non-terminal states
        if state in ('RUNNING', 'PENDING', 'REQUEUED'):
            continue

        secs = _parse_elapsed_seconds(elapsed)
        gpus = _parse_count(alloc_gres)

        h = hist[user]
        h['total'] += 1
        if state == 'COMPLETED':
            h['completed'] += 1
        elif 'FAIL' in state or 'MEMORY' in state or 'TIMEOUT' in state:
            h['failed'] += 1
        elif 'CANCEL' in state:
            h['cancelled'] += 1
        h['gpu_secs'] += gpus * secs
        h['elapsed_secs'] += secs
        if partition.strip():
            h['partitions'][partition.strip()] += 1

    # ── current squeue state ──────────────────────────────────────────
    running_gpus: Dict[str, int] = defaultdict(int)
    pending_jobs: Dict[str, int] = defaultdict(int)
    try:
        sq = subprocess.check_output(
            ['squeue', '-o', '%u|%T|%b', '-h', '--states=RUNNING,PENDING'],
            text=True, stderr=subprocess.DEVNULL,
        )
        for ln in sq.splitlines():
            if not ln.strip():
                continue
            u, st, gres = (ln.split('|') + ['', '', ''])[:3]
            u = u.strip()
            mt = GPU_RE_NAMED.search(gres.strip())
            if mt:
                g = int(mt.group(2))
            else:
                flex = FLEX_RE.search(gres.strip())
                g = int(flex.group(1)) if flex else 0
            if st.strip() == 'RUNNING':
                running_gpus[u] += g
            else:
                pending_jobs[u] += 1
    except Exception:
        pass

    # ── merge & sort ──────────────────────────────────────────────────
    all_users = set(hist.keys()) | set(running_gpus.keys()) | set(pending_jobs.keys())
    result = []
    for user in all_users:
        h = hist.get(user, {})
        total = h.get('total', 0)
        gpu_hours = h.get('gpu_secs', 0.0) / 3600.0
        avg_secs = (h.get('elapsed_secs', 0) // total) if total else 0
        parts_d = h.get('partitions', {})
        top_part = max(parts_d, key=parts_d.get) if parts_d else ''
        result.append({
            'user':         user,
            'running_gpus': running_gpus.get(user, 0),
            'pending_jobs': pending_jobs.get(user, 0),
            'total_jobs':   total,
            'completed':    h.get('completed', 0),
            'failed':       h.get('failed', 0),
            'cancelled':    h.get('cancelled', 0),
            'gpu_hours':    gpu_hours,
            'avg_secs':     avg_secs,
            'top_partition': top_part,
        })

    result.sort(key=lambda r: (-r['running_gpus'], -r['gpu_hours']))
    return result


# ── Usage time-series ──────────────────────────────────────────────────────────

# Stats cache: key → (monotonic_ts, value)
# TTLs: timeseries/breakdown = 60 s, filter lists = 300 s.
# Datetimes are quantised to 60-second slots so that repeated calls with
# slightly different datetime.now() values still hit the same cache entry.
_stats_cache: Dict[tuple, Tuple[float, object]] = {}


def _stats_get(key: tuple, ttl: float, fn) -> object:
    """Return cached value or call fn() and cache the result."""
    now = _time.monotonic()
    cached = _stats_cache.get(key)
    if cached:
        ts, val = cached
        if now - ts < ttl:
            return val
    val = fn()
    _stats_cache[key] = (now, val)
    return val


def _dt_slot(dt: datetime, slot_secs: int = 60) -> int:
    """Quantise a datetime to a slot index so nearby timestamps share a key."""
    return int(dt.timestamp() / slot_secs)


def _parse_sacct_dt(s: str) -> Optional[datetime]:
    s = (s or '').strip()
    if not s or s in ('Unknown', 'None', 'N/A', 'NONE', 'none'):
        return None
    try:
        return datetime.strptime(s.replace('T', ' ').split('.')[0], '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def _rel_label(dt: datetime, now: datetime) -> str:
    """Human-friendly relative label: 'now', '~30m', '~6h', '1d', '7d'."""
    secs = (now - dt).total_seconds()
    if secs < 90:
        return 'now'
    if secs < 3600:
        return f'{int(secs / 60)}m'
    if secs < 86400:
        return f'{int(secs / 3600)}h'
    return f'{int(secs / 86400)}d'


def _empty_ts(n: int, start: datetime, end: datetime) -> Dict:
    total_secs = max((end - start).total_seconds(), 1)
    bucket_secs = total_secs / n
    now = datetime.now()
    labels = [
        _rel_label(start + timedelta(seconds=i * bucket_secs), now)
        for i in range(n)
    ]
    return {'buckets': [0.0] * n, 'labels': labels,
            'peak': 0.0, 'total_gpu_hours': 0.0, 'n_jobs': 0}


def _bucket_jobs(
    jobs: List[Tuple[datetime, datetime, int]],
    start: datetime,
    end: datetime,
    n: int,
) -> Dict:
    total_secs = max((end - start).total_seconds(), 1)
    bucket_secs = total_secs / n
    buckets = [0.0] * n
    for js, je, gpus in jobs:
        js = max(js, start)
        je = min(je, end)
        if je <= js:
            continue
        i0 = int((js - start).total_seconds() / bucket_secs)
        i1 = int((je - start).total_seconds() / bucket_secs)
        for i in range(max(0, i0), min(n, i1 + 1)):
            b0 = start + timedelta(seconds=i * bucket_secs)
            b1 = b0 + timedelta(seconds=bucket_secs)
            overlap = (min(je, b1) - max(js, b0)).total_seconds()
            if overlap > 0:
                buckets[i] += gpus * overlap / bucket_secs
    now = datetime.now()
    labels = [
        _rel_label(start + timedelta(seconds=i * bucket_secs), now)
        for i in range(n)
    ]
    peak = max(buckets) if buckets else 0.0
    total_gpu_hours = sum(b * bucket_secs for b in buckets) / 3600.0
    return {'buckets': buckets, 'labels': labels,
            'peak': peak, 'total_gpu_hours': total_gpu_hours, 'n_jobs': len(jobs)}


# ── Issue / trouble stats ──────────────────────────────────────────────────────

_TROUBLE_STATES = {'FAILED', 'TIMEOUT', 'OUT_OF_MEMORY', 'NODE_FAIL'}
_issue_cache: Dict[int, Tuple[float, Dict]] = {}   # hours → (ts, data)


def fetch_issue_stats(hours: int = 24) -> Dict:
    """Return failed/timeout/OOM counts per GPU model (partition) and per node
    for the last *hours* hours.  Cached for 60 s.

    Result shape::
        {
          'by_model': {'A100': {'failed': 2, 'timeout': 1, 'oom': 0, 'total': 3}},
          'by_node':  {'log-node01': {'failed': 2, 'timeout': 1, 'oom': 0, 'total': 3}},
        }
    """
    now = _time.monotonic()
    cached = _issue_cache.get(hours)
    if cached and now - cached[0] < 60.0:
        return cached[1]

    start_s = (datetime.now() - timedelta(hours=hours)).strftime(_DT_FMT)
    base_args = ['sacct', '-S', start_s, '--parsable2', '--noheader',
                 '--format=JobID,Partition,NodeList,State,AllocTRES']
    out = None
    for cmd in [base_args + ['--allusers'], base_args]:
        try:
            out = subprocess.check_output(
                cmd, text=True, stderr=subprocess.DEVNULL, timeout=30,
            )
            break
        except Exception:
            continue

    by_model: Dict[str, Dict] = defaultdict(lambda: {'failed': 0, 'timeout': 0, 'oom': 0, 'total': 0})
    by_node:  Dict[str, Dict] = defaultdict(lambda: {'failed': 0, 'timeout': 0, 'oom': 0, 'total': 0})

    if out:
        for ln in (out or '').splitlines():
            if not ln.strip():
                continue
            parts = (ln.split('|') + [''] * 5)[:5]
            jid, partition, nodelist, state, alloc_tres = parts
            if '.' in jid:
                continue
            state_base = state.split()[0].upper()
            if state_base not in _TROUBLE_STATES:
                continue
            if _parse_count(alloc_tres) == 0:
                continue
            slot = 'oom' if state_base == 'OUT_OF_MEMORY' else state_base.lower()
            model = partition.strip() or 'unknown'
            by_model[model][slot] += 1
            by_model[model]['total'] += 1
            for node in nodelist.strip().split(','):
                node = node.strip()
                if node and node not in ('None', ''):
                    by_node[node][slot] += 1
                    by_node[node]['total'] += 1

    result = {'by_model': dict(by_model), 'by_node': dict(by_node)}
    _issue_cache[hours] = (now, result)
    return result


_issue_ts_cache: Dict[Tuple[int, int], Tuple[float, Dict]] = {}


def fetch_issue_timeseries(hours: int = 24, n_buckets: int = 0) -> Dict:
    """Return issue counts per GPU model bucketed into time intervals.

    Result shape::
        {
          'models': ['A100-80GB', ...],
          'bucket_labels': ['-24h', '-18h', ...],
          'data': {'A100-80GB': [{'failed':2,'timeout':1,'oom':0,'total':3}, ...]},
        }
    """
    if n_buckets <= 0:
        if hours <= 24:
            n_buckets = 6
        elif hours <= 48:
            n_buckets = 8
        else:
            n_buckets = 7   # 7d → daily

    cache_key = (hours, n_buckets)
    now_mono = _time.monotonic()
    cached = _issue_ts_cache.get(cache_key)
    if cached and now_mono - cached[0] < 60.0:
        return cached[1]

    now_dt = datetime.now()
    start = now_dt - timedelta(hours=hours)
    start_s = start.strftime(_DT_FMT)

    bucket_size = timedelta(hours=hours) / n_buckets
    bucket_starts = [start + bucket_size * i for i in range(n_buckets)]
    fmt = '%H:%M' if hours <= 48 else '%m/%d'
    bucket_labels = [b.strftime(fmt) for b in bucket_starts]

    base_args = ['sacct', '-S', start_s, '--parsable2', '--noheader',
                 '--format=JobID,Partition,State,AllocTRES,End']
    out = None
    for cmd in [base_args + ['--allusers'], base_args]:
        try:
            out = subprocess.check_output(
                cmd, text=True, stderr=subprocess.DEVNULL, timeout=30,
            )
            break
        except Exception:
            continue

    models_seen: List[str] = []
    raw: Dict[str, List[Dict]] = {}

    if out:
        for ln in out.splitlines():
            if not ln.strip():
                continue
            parts = (ln.split('|') + [''] * 5)[:5]
            jid, partition, state, alloc_tres, end_s = parts
            if '.' in jid:
                continue
            state_base = state.split()[0].upper()
            if state_base not in _TROUBLE_STATES:
                continue
            if _parse_count(alloc_tres) == 0:
                continue
            try:
                end_dt = datetime.strptime(end_s.strip(), _DT_FMT)
            except ValueError:
                continue
            # Bucket by end time (binary search style)
            bucket_idx = n_buckets - 1
            for i in range(n_buckets - 1, -1, -1):
                if end_dt >= bucket_starts[i]:
                    bucket_idx = i
                    break
            slot = 'oom' if state_base == 'OUT_OF_MEMORY' else state_base.lower()
            model = partition.strip() or 'unknown'
            if model not in raw:
                raw[model] = [{'failed': 0, 'timeout': 0, 'oom': 0, 'total': 0}
                               for _ in range(n_buckets)]
                models_seen.append(model)
            raw[model][bucket_idx][slot] += 1
            raw[model][bucket_idx]['total'] += 1

    model_list = sorted(
        models_seen,
        key=lambda m: sum(raw[m][i]['total'] for i in range(n_buckets)),
        reverse=True,
    )
    result = {
        'models': model_list,
        'bucket_labels': bucket_labels,
        'data': {m: raw[m] for m in model_list},
    }
    _issue_ts_cache[cache_key] = (now_mono, result)
    return result


# ── Shared raw-rows cache (one sacct call serves both analyses) ────────────────

# Each row: (jid, user, partition, nodelist, start_dt, end_dt, gpus, elapsed_secs, alloc_gres)
_StatsRow = Tuple[str, str, str, str, datetime, Optional[datetime], int, int, str]


def _fetch_stats_rows(start_dt: datetime, end_dt: datetime) -> List[_StatsRow]:
    """Single sacct call that fetches every field needed by both analyses.

    Tries --allusers first (requires accounting admin rights on some clusters);
    falls back to no user filter if that fails, which on most clusters returns
    all users via the accounting DB without needing elevated permissions.
    """
    start_s = start_dt.strftime(_DT_FMT)
    end_s   = end_dt.strftime(_DT_FMT)
    base_args = ['sacct', '-S', start_s, '-E', end_s,
                 '--parsable2', '--noheader',
                 '--format=JobID,User,Partition,NodeList,Start,End,Elapsed,AllocTRES']
    out = None
    for cmd in [base_args + ['--allusers'], base_args]:
        try:
            out = subprocess.check_output(
                cmd, text=True, stderr=subprocess.DEVNULL, timeout=45,
            )
            break
        except Exception:
            continue
    if out is None:
        return []

    rows: List[_StatsRow] = []
    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + [''] * 8)[:8]
        jid, user, partition, nodelist, start_raw, end_raw, elapsed, alloc_gres = parts
        if '.' in jid:
            continue                   # skip step rows
        gpus = _parse_count(alloc_gres)
        if gpus == 0:
            continue
        js = _parse_sacct_dt(start_raw)
        if not js:
            continue
        je  = _parse_sacct_dt(end_raw)  # None = still running
        sec = _parse_elapsed_seconds(elapsed)
        rows.append((jid, user.strip(), partition.strip(), nodelist.strip(),
                     js, je, gpus, sec, alloc_gres.strip()))
    return rows


def fetch_stats_rows_chunked(
    start_dt: datetime,
    end_dt: datetime,
    on_chunk,           # Callable[[List[_StatsRow], int, int], None]
) -> List[_StatsRow]:
    """Fetch raw rows in parallel chunks, calling on_chunk(rows_so_far, done, total) as each arrives.

    If a fresh cache entry exists the callback is called once with the full
    result and returns immediately.  Otherwise the window is split into up to
    7 chunks fetched in parallel (≤ 4 threads), giving progressive UI updates.
    """
    cache_key = ('rows', _dt_slot(start_dt), _dt_slot(end_dt))
    cached = _stats_cache.get(cache_key)
    if cached:
        ts, rows = cached
        if _time.monotonic() - ts < 60.0:
            on_chunk(rows, 1, 1)
            return rows  # type: ignore[return-value]

    total_days = max(1.0, (end_dt - start_dt).total_seconds() / 86400.0)
    # ~7 chunks at most; each chunk ≥ 1 day
    chunk_days = max(1, int(total_days / 7 + 0.99))
    chunk_secs = chunk_days * 86400
    n_chunks   = max(1, int((end_dt - start_dt).total_seconds() / chunk_secs + 0.99))

    chunk_ranges = [
        (start_dt + timedelta(seconds=i * chunk_secs),
         min(end_dt, start_dt + timedelta(seconds=(i + 1) * chunk_secs)))
        for i in range(n_chunks)
    ]

    all_rows: List[_StatsRow] = []
    done_count = [0]
    lock = _threading.Lock()

    with _ThreadPoolExecutor(max_workers=min(n_chunks, 4)) as pool:
        futures = {pool.submit(_fetch_stats_rows, s, e): i
                   for i, (s, e) in enumerate(chunk_ranges)}
        for fut in _as_completed(futures):
            rows = fut.result()
            with lock:
                all_rows.extend(rows)
                done_count[0] += 1
                snapshot = list(all_rows)
                done = done_count[0]
            on_chunk(snapshot, done, n_chunks)

    _stats_cache[cache_key] = (_time.monotonic(), all_rows)
    return all_rows


def _compute_timeseries(
    all_rows: List[_StatsRow],
    start_dt: datetime,
    end_dt: datetime,
    dimension: str,
    filter_val: str,
    n_buckets: int,
) -> Dict:
    now = datetime.now()
    jobs: List[Tuple[datetime, datetime, int]] = []
    for _, user, partition, nodelist, js, je, gpus, _, _ in all_rows:
        if dimension == 'partition' and filter_val and partition != filter_val:
            continue
        if dimension == 'user' and filter_val and user != filter_val:
            continue
        if dimension == 'node' and filter_val and filter_val not in nodelist:
            continue
        jobs.append((js, je or min(now, end_dt), gpus))
    return _bucket_jobs(jobs, start_dt, end_dt, n_buckets)


def _compute_breakdown(all_rows: List[_StatsRow], dimension: str) -> List[Dict]:
    groups: Dict[str, Dict] = defaultdict(lambda: {'gpu_secs': 0.0, 'jobs': 0})
    for _, user, partition, nodelist, _, _, gpus, secs, alloc_gres in all_rows:
        if dimension == 'partition':
            grp: Optional[str] = partition or 'unknown'
        elif dimension == 'node':
            grp = nodelist.split(',')[0] or 'unknown'
        elif dimension == 'user':
            grp = user or 'unknown'
        else:
            mm = GPU_RE_NAMED.search(alloc_gres)
            grp = _norm(mm.group(1)) if mm else ('GPU' if gpus else None)
        if not grp:
            continue
        groups[grp]['gpu_secs'] += gpus * secs
        groups[grp]['jobs'] += 1

    result = [
        {'name': k, 'gpu_hours': v['gpu_secs'] / 3600.0, 'jobs': v['jobs']}
        for k, v in groups.items()
    ]
    result.sort(key=lambda r: -r['gpu_hours'])
    return result


def fetch_usage_timeseries(
    start_dt: datetime,
    end_dt: datetime,
    dimension: str = 'cluster',
    filter_val: str = '',
    n_buckets: int = 84,
) -> Dict:
    """Return bucketed GPU-count timeseries (cached). Used when rows already cached."""
    key = ('ts', _dt_slot(start_dt), _dt_slot(end_dt), dimension, filter_val, n_buckets)
    rows = _stats_cache.get(('rows', _dt_slot(start_dt), _dt_slot(end_dt)))
    if rows and _time.monotonic() - rows[0] < 60.0:
        return _stats_get(key, 60.0, lambda: _compute_timeseries(  # type: ignore[return-value]
            rows[1], start_dt, end_dt, dimension, filter_val, n_buckets))
    # rows not cached: fetch them (no chunking here — used by filter/dimension switches)
    raw = _fetch_stats_rows(start_dt, end_dt)
    _stats_cache[('rows', _dt_slot(start_dt), _dt_slot(end_dt))] = (_time.monotonic(), raw)
    return _compute_timeseries(raw, start_dt, end_dt, dimension, filter_val, n_buckets)


def fetch_breakdown_stats(
    start_dt: datetime,
    end_dt: datetime,
    dimension: str = 'cluster',
) -> List[Dict]:
    """Return per-group GPU-hour stats (cached). Used when rows already cached."""
    rows = _stats_cache.get(('rows', _dt_slot(start_dt), _dt_slot(end_dt)))
    if rows and _time.monotonic() - rows[0] < 60.0:
        return _compute_breakdown(rows[1], dimension)  # type: ignore[return-value]
    raw = _fetch_stats_rows(start_dt, end_dt)
    _stats_cache[('rows', _dt_slot(start_dt), _dt_slot(end_dt))] = (_time.monotonic(), raw)
    return _compute_breakdown(raw, dimension)


def fetch_filter_users() -> List[str]:
    """Return sorted list of users seen in sacct over the past 30 days (cached 5 min)."""
    return _stats_get(('users',), 300.0, _fetch_filter_users)  # type: ignore[return-value]


def _fetch_filter_users() -> List[str]:
    start = (datetime.now() - timedelta(days=30)).strftime(_DT_FMT)
    try:
        out = subprocess.check_output(
            ['sacct', '-S', start, '--parsable2', '--noheader',
             '--allusers', '--format=User'],
            text=True, stderr=subprocess.DEVNULL, timeout=10,
        )
        return sorted({ln.strip() for ln in out.splitlines()
                       if ln.strip() and '.' not in ln.strip()})
    except Exception:
        return []


# ── Live GPU / resource stats ──────────────────────────────────────────────────

def fetch_job_gpu_stats(job_id: str, nodelist: str) -> Dict:
    """Fetch live GPU stats for a running job via srun --overlap + sstat.

    GPU query uses nvidia-smi executed on the job's allocated nodes through
    ``srun --overlap`` — fast (< 5 s), no profiling overhead.
    Falls back to a local nvidia-smi query if srun fails.

    Returns {'gpu_rows': [...], 'sstat': {...}, 'error': str|None}.
    Each gpu_row is a list of strings:
      [hostname, idx, name, gpu_pct, mem_pct, mem_used_mib, mem_total_mib, temp_c, power_w]
    """
    _NV_FMT = ('index,name,utilization.gpu,utilization.memory,'
               'memory.used,memory.total,temperature.gpu,power.draw')

    gpu_rows: List[List[str]] = []
    error: Optional[str] = None

    # ── srun on allocated nodes ────────────────────────────────────────────
    if nodelist and nodelist not in ('None', 'none', ''):
        try:
            raw = subprocess.check_output(
                ['srun', '--quiet', '--overlap',
                 f'--jobid={job_id}', '--nodes=1-',
                 'nvidia-smi',
                 f'--query-gpu={_NV_FMT}',
                 '--format=csv,noheader,nounits'],
                text=True, stderr=subprocess.STDOUT, timeout=15,
            )
            host = nodelist.split(',')[0].rstrip('[0123456789]').rstrip('-')
            for ln in raw.splitlines():
                ln = ln.strip()
                if not ln or ',' not in ln:
                    continue
                parts = [p.strip() for p in ln.split(',')]
                if len(parts) >= 8:
                    gpu_rows.append([host] + parts[:8])
        except subprocess.TimeoutExpired:
            error = 'srun timed out (>15 s)'
        except subprocess.CalledProcessError as e:
            error = (e.output or str(e)).strip().splitlines()[0][:80]
        except Exception as e:
            error = str(e)[:80]

    # ── local fallback ────────────────────────────────────────────────────
    if not gpu_rows:
        try:
            raw = subprocess.check_output(
                ['nvidia-smi',
                 f'--query-gpu={_NV_FMT}',
                 '--format=csv,noheader,nounits'],
                text=True, stderr=subprocess.DEVNULL, timeout=5,
            )
            import socket
            host = socket.gethostname()
            for ln in raw.splitlines():
                ln = ln.strip()
                if not ln or ',' not in ln:
                    continue
                parts = [p.strip() for p in ln.split(',')]
                if len(parts) >= 8:
                    gpu_rows.append([host] + parts[:8])
            if gpu_rows:
                error = None   # local query succeeded; clear srun error
        except Exception:
            pass

    # ── sstat (CPU / memory of running job) ──────────────────────────────
    sstat: Dict[str, str] = {}
    try:
        out = subprocess.check_output(
            ['sstat', '-j', job_id, '--parsable2', '--noheader',
             '--format=JobID,AveCPU,AveRSS,AveVMSize,MaxRSS,MaxVMSize'],
            text=True, stderr=subprocess.DEVNULL, timeout=8,
        )
        for ln in out.splitlines():
            parts = (ln.split('|') + [''] * 6)[:6]
            if parts[0].strip().split('.')[0] == job_id:
                sstat = {
                    'AveCPU':    parts[1].strip(),
                    'AveRSS':    parts[2].strip(),
                    'AveVMSize': parts[3].strip(),
                    'MaxRSS':    parts[4].strip(),
                    'MaxVMSize': parts[5].strip(),
                }
                break
    except Exception:
        pass

    return {'gpu_rows': gpu_rows, 'sstat': sstat, 'error': error}


# ── Partitions ────────────────────────────────────────────────────────────────

def get_partitions() -> List[str]:
    try:
        out = subprocess.check_output(
            ['sinfo', '-h', '-o', '%P'],
            text=True, stderr=subprocess.DEVNULL,
        )
        return sorted({p.strip().rstrip('*') for p in out.splitlines() if p.strip()})
    except Exception:
        return []
