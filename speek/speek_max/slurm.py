"""slurm.py — SLURM data layer for speek-max-2. All stdlib only."""
from __future__ import annotations

import re
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# ── Regex constants ────────────────────────────────────────────────────────────

GPU_RE_NAMED = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)
FLEX_RE = re.compile(r'gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', re.IGNORECASE)
GPU_RE = re.compile(r'gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', re.IGNORECASE)

_LOG_RE = re.compile(r'StdOut=(\S+)')

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
    'A100-80GB':  80,
    'A100-40GB':  40,
    '4A100':      40,
    'L40S':       48,
    'L40':        48,
    'A6000':      48,
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
    """Returns {model: {Total, Used, Free, VRAM, Nodes}} from scontrol show node."""
    try:
        out = subprocess.check_output(
            ['scontrol', 'show', 'node', '--oneliner'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return {}

    agg: Dict[str, Dict] = defaultdict(lambda: {'Total': 0, 'Used': 0, 'Nodes': []})

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

        used = min(used, total)
        agg[model]['Total'] += total
        agg[model]['Used']  += used
        agg[model]['Nodes'].append((node, state))

    result = {}
    for m, d in agg.items():
        vram = _MODEL_VRAM.get(m)
        if vram is None:
            mv = re.search(r'(\d+)GB', m, re.IGNORECASE)
            if mv:
                vram = int(mv.group(1))
        result[m] = {
            'Total': d['Total'],
            'Used':  d['Used'],
            'Free':  d['Total'] - d['Used'],
            'VRAM':  vram,
            'Nodes': sorted(d['Nodes'], key=lambda x: x[0]),
        }
    return result


# ── Priority / fairshare ───────────────────────────────────────────────────────

def fetch_all_priorities() -> Dict[str, Dict]:
    """Return {job_id: {total, age, fairshare, jobsize, qos, partition, user}}
    for all pending jobs via sprio."""
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

    {
        'by_model': {model: {'R': N, 'PD': N}},
        'total_R': N, 'total_PD': N,
        'active_users': N,
    }
    """
    try:
        out = subprocess.check_output(
            ['squeue', '-o', '%T|%u|%b', '-h',
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
        parts = (ln.split('|') + ['', '', ''])[:3]
        state, user, gres = parts[0].strip(), parts[1].strip(), parts[2].strip()

        mt = GPU_RE_NAMED.search(gres)
        if mt:
            model = _norm(mt.group(1))
        else:
            flex = FLEX_RE.search(gres)
            model = 'GPU' if flex else None

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

    return {
        'by_model': dict(by_model),
        'total_R': total_R,
        'total_PD': total_PD,
        'active_users': len(users),
        'user_gpus': dict(user_gpus),
    }


# ── Queue ──────────────────────────────────────────────────────────────────────

def _gpu_count(gres: str) -> str:
    mt = GPU_RE.search(gres or '')
    return mt.group(1) if mt else '-'


def fetch_queue() -> List[Tuple]:
    """Returns list of row tuples: (jobid, user, name, partition, gpus, state, elapsed)."""
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
    """Returns [(jid, name, partition, gpus, state, elapsed, eta), ...]."""
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
    return rows


def fetch_job_details(job_id: str) -> Dict[str, str]:
    """Parse `scontrol show job <id>` into a flat key→value dict."""
    try:
        out = subprocess.check_output(
            ['scontrol', 'show', 'job', job_id],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return {}
    result: Dict[str, str] = {}
    for token in re.split(r'\s+', out):
        if '=' in token:
            k, _, v = token.partition('=')
            result[k.strip()] = v.strip()
    return result


def get_job_log_path(job_id: str) -> Optional[str]:
    """Get StdOut path for a job from scontrol."""
    try:
        out = subprocess.check_output(
            ['scontrol', 'show', 'job', job_id],
            text=True, stderr=subprocess.DEVNULL,
        )
        mt = _LOG_RE.search(out)
        if mt:
            path = mt.group(1)
            path = path.replace('%j', job_id)
            return path
    except Exception:
        pass
    return None


# ── History ───────────────────────────────────────────────────────────────────

def fetch_history(days: int) -> List[Tuple]:
    """Returns sacct history tuples: (jid, name, partition, start, elapsed, state, exit_code)."""
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%S')
    try:
        out = subprocess.check_output(
            ['sacct', '-S', start, '--parsable2', '--noheader',
             '--format=JobID,JobName,Partition,Start,Elapsed,State,ExitCode'],
            text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []
    rows = []
    for ln in out.splitlines():
        if not ln.strip():
            continue
        parts = (ln.split('|') + [''] * 7)[:7]
        jid, name, part, start_t, elapsed, state, exit_code = parts
        if '.' in jid:
            continue
        rows.append((
            jid.strip(),
            name.strip(),
            part.strip(),
            start_t.strip(),
            elapsed.strip(),
            state.strip(),
            exit_code.strip(),
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
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%S')

    # ── sacct history ──────────────────────────────────────────────────
    try:
        out = subprocess.check_output(
            ['sacct', '-S', start, '--parsable2', '--noheader', '--allusers',
             '--format=User,State,Elapsed,AllocGRES,Partition'],
            text=True, stderr=subprocess.DEVNULL,
            timeout=8,
        )
    except Exception:
        out = ''

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
