#!/usr/bin/env python3
import subprocess, re, argparse, time
from collections import defaultdict
from typing import Optional, Dict, Set, Tuple, List
from datetime import datetime, timedelta

from rich import print
from rich.table import Table
from rich.align import Align
from rich.text import Text
from rich.live import Live
from rich.console import Group

# predict_pending_job_eta used only in my_jobs_table for PENDING ETA
try:
    from speek.slurm_predictor import predict_pending_job_eta
except Exception:
    def predict_pending_job_eta(jobid: int, details: bool = False):
        return (None, {}) if details else None

# ---------------------- CLI ----------------------
parser = argparse.ArgumentParser(
    description="Cluster Usage Lite (live/one-shot; bars; colored availability; users; a-priori 'Will it pend'; my jobs with ETA; no disk)."
)
parser.add_argument(
    '-l','--live', action='store_true',
    help='Live display, refresh every 1s.'
)
parser.add_argument(
    '-u','--user', type=str, default=None,
    help='User to show jobs for (default: current user).'
)
parser.add_argument(
    '-n','--nodes', action='store_true',
    help='Show per-node GPU bars grouped by partition.'
)
parser.set_defaults(live=False, nodes=False)

args = parser.parse_args()

# ---------------------- Regex ----------------------
GPU_TOTAL_RE = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)
GPU_USED_RE  = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)
REQ_GPU_RE   = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)
# Flexible: matches gpu:MODEL:N, gpu:N, gres/gpu:N — for node-level counting
GPU_FLEX_RE  = re.compile(r'gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', re.IGNORECASE)

# ---------------------- Helpers ----------------------
def normalize_model(m: str) -> str:
    m = (m or "").strip()
    if m.startswith('A100-SXM4-80GB'): return 'A100-80GB'
    if m.startswith('A100-SXM4-40GB'): return '4A100'
    if m.startswith('A100-PCIE-40GB'): return 'A100-40GB'
    if m.upper().startswith('RTX2080TI'): return '2080ti'
    if m == 'RTX3090': return '3090'
    return m

def looks_like_gpu_model(s: str) -> bool:
    if not s: return False
    s = s.upper()
    return any(tag in s for tag in ['A100','A6000','A5000','H100','H200','L40','3090','4090','TITAN','2080'])

def _parse_gres_count(gres_str: str) -> int:
    """Extract GPU count from a gres string; strips (IDX:...) suffixes first."""
    s = re.sub(r'\(IDX:[^)]*\)', '', gres_str or '')
    m = GPU_FLEX_RE.search(s)
    return int(m.group(1)) if m else 0

def usage_emoji(pct: float) -> str:
    if pct >= 100: return '☠️ '
    if pct > 90:   return '🔥'
    if pct == 0:   return '🏖️ '
    if pct < 10:   return '❄️ '
    return ''

def _run_sinfo(cols: str) -> str:
    return subprocess.check_output(['sinfo','-N','-O', cols], text=True)

def parse_sinfo_rows():
    """
    Try: NodeHost,Partition,Gres,GresUsed
    Fallback: NodeHost,Gres,GresUsed
    -> list of (host, partition_or_None, gres, gres_used)
    """
    for cols, ncols in [
        ('NodeHost,Partition,Gres,GresUsed', 4),
        ('NodeHost,Gres,GresUsed', 3),
    ]:
        try:
            raw = _run_sinfo(cols)
            lines = [ln.rstrip() for ln in raw.splitlines() if ln.strip()]
            if not lines: return []
            def split_row(s):
                parts = re.split(r'\s{2,}', s.strip())
                if len(parts) < ncols:
                    s2 = re.sub(r'(gpu:)', r'  \1', s).strip()
                    parts = re.split(r'\s{2,}', s2)
                parts += [''] * (ncols - len(parts))
                return parts[:ncols]
            out = []
            for ln in lines[1:]:
                p = split_row(ln)
                if ncols == 4:
                    host, part, gres, gres_used = p
                else:
                    host, gres, gres_used = p; part = None
                out.append((
                    host.strip(),
                    (part or '').strip() or None,
                    (gres or '').strip(),
                    (gres_used or '').strip()
                ))
            return out
        except:
            continue
    raise RuntimeError("Failed to parse sinfo output.")

def get_partition_weights():
    try:
        out = subprocess.check_output(['scontrol','show','partition'], text=True)
    except:
        return {}
    weights = {}
    for b in re.split(r'\bPartitionName=', out):
        b = b.strip()
        if not b: continue
        name, rest = (b.split(None, 1) + [''])[:2]
        m = re.search(r'TRESBillingWeights=(\S+)', rest)
        if not m: continue
        for kv in m.group(1).split(','):
            if '=' not in kv: continue
            k, v = kv.split('=', 1)
            if k.lower().startswith('gres/gpu'):
                try:
                    weights[name.strip()] = float(re.sub(r'[^0-9.\-]', '', v))
                except:
                    pass
    return weights

def aggregate(rows):
    """Aggregate GPU totals/used per model and track partitions."""
    per_node_model = {}
    model_parts = defaultdict(set)
    for host, part, gres, gres_used in rows:
        mt = GPU_TOTAL_RE.search(gres or '')
        mu = GPU_USED_RE.search(gres_used or '')
        if not mt: continue
        raw_model = mt.group(1)
        total = int(mt.group(2))
        used = 0
        if mu and normalize_model(mu.group(1)) == normalize_model(raw_model):
            try: used = int(mu.group(2))
            except: used = 0
        model = normalize_model(raw_model)
        key = (host, model)
        if key in per_node_model:
            pt, pu = per_node_model[key]
            per_node_model[key] = (max(pt, total), max(pu, used))
        else:
            per_node_model[key] = (total, used)
        if part and part != '(null)':
            for p in re.split(r'[,\s]+', part):
                if p: model_parts[model].add(p)
    agg = defaultdict(lambda: {'Total': 0, 'Used': 0})
    for (_, model), (t, u) in per_node_model.items():
        agg[model]['Total'] += t
        agg[model]['Used']  += u
    return dict(agg), model_parts

# -------- Per-node / per-partition view --------

def aggregate_nodes_by_partition(rows) -> Dict[str, List[Tuple[str, int, int]]]:
    """
    Returns {partition: [(host, total_gpus, used_gpus), ...]} for GPU nodes only.
    Nodes that appear in multiple partitions show up once per partition.
    Sorted per partition by used GPUs descending, then hostname.
    """
    seen: Dict[Tuple[str, str], Tuple[int, int]] = {}
    for host, part, gres, gres_used in rows:
        total = _parse_gres_count(gres)
        if total == 0:
            continue
        used = min(_parse_gres_count(gres_used), total)
        p = part or 'unknown'
        key = (host, p)
        prev = seen.get(key)
        if prev is None or used > prev[1]:
            seen[key] = (total, used)
    result: Dict[str, List[Tuple[str, int, int]]] = defaultdict(list)
    for (host, p), (total, used) in seen.items():
        result[p].append((host, total, used))
    return {
        p: sorted(nodes, key=lambda x: (-x[2], x[0]))
        for p, nodes in sorted(result.items())
    }

def _node_bar(used: int, total: int, width: Optional[int] = None) -> Text:
    """Compact block progress bar. Default width = min(total, 16) so 1 char ≈ 1 GPU."""
    w = width if width is not None else min(max(total, 1), 16)
    pct = used / total if total else 0.0
    filled = int(round(pct * w))
    if pct <= 0.30:
        color = 'blue'
    elif pct <= 0.70:
        color = 'orange1'
    else:
        color = 'red'
    bar = Text()
    bar.append('█' * filled,      style=f'bold {color}')
    bar.append('░' * (w - filled), style='dim')
    return bar

def build_node_table(rows) -> Optional['Align']:
    """Per-partition, per-node GPU bar table. Returns None if no GPU nodes found."""
    node_data = aggregate_nodes_by_partition(rows)
    if not node_data:
        return None
    def _avail_color(free: int, total: int) -> str:
        pct = free / total if total else 0.0
        if pct >= 0.50: return 'bold green'
        if pct >= 0.20: return 'bold yellow'
        return 'bold red'

    def _row_style(free: int, total: int) -> str:
        pct = free / total if total else 0.0
        if pct >= 0.50: return 'on color(22)'   # dark green tint: submit here
        if pct < 0.20:  return 'dim'             # nearly full: recede
        return ''

    table = Table(title='Node GPU Usage by Partition', show_header=True, header_style='bold')
    table.add_column('Partition / Node', no_wrap=True, min_width=14)
    table.add_column('Free', justify='right', min_width=5)
    table.add_column('Bar', no_wrap=True, min_width=16)
    table.add_column('/Tot', justify='right', min_width=4)
    table.add_column('', width=3)

    for part, nodes in node_data.items():
        part_total = sum(t for _, t, _ in nodes)
        part_used  = sum(u for _, _, u in nodes)
        part_free  = part_total - part_used
        pct        = part_used / part_total if part_total else 0.0
        # partition header: always full brightness (it's a summary, not a target)
        table.add_row(
            Text(part, style='bold cyan'),
            Text(str(part_free), style=_avail_color(part_free, part_total)),
            _node_bar(part_used, part_total, width=20),
            Text(f'/{part_total}', style='dim'),
            Text(usage_emoji(pct * 100)),
        )
        for host, total, used in nodes:
            free  = total - used
            pct_n = used / total if total else 0.0
            table.add_row(
                Text(f'  {host}'),
                Text(str(free), style=_avail_color(free, total)),
                _node_bar(used, total),
                Text(f'/{total}', style='dim'),
                Text(usage_emoji(pct_n * 100)) if pct_n >= 1.0 or pct_n == 0.0 else '',
                style=_row_style(free, total),
            )
        table.add_section()
    return Align(table, align='center')

# -------- Simple colored progress bar function --------
def make_colored_progress_bar(pct: float, text: str) -> Text:
    if pct <= 0.30:
        bg_color = "blue";    text_color = "white"
    elif pct <= 0.70:
        bg_color = "orange1"; text_color = "black"
    else:
        bg_color = "red";     text_color = "white"
    width = len(text)
    filled_chars = int(round(pct * width))
    result = Text()
    result.append("[")
    for i in range(width):
        ch = text[i]
        if i >= (width - filled_chars):
            result.append(ch, style=f"bold {text_color} on {bg_color}")
        else:
            result.append(ch, style="bold white")
    result.append("]")
    return result

# ---------------------- Users (RUNNING) via squeue (model-aware) ----------------------
def running_user_gpu_usage_model_aware() -> Tuple[Dict[str, Dict[str, int]], int, int]:
    per_model_user: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total_used = 0
    users_set = set()
    try:
        out = subprocess.check_output(
            ['squeue','-t','R','-o','%u|%P|%b','-h'],
            text=True, stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        out = ""

    for ln in out.splitlines():
        if not ln.strip(): continue
        user, part, gres = (ln.split('|') + ['','',''])[:3]
        user = user.strip()
        part = (part or '').strip()
        gres = (gres or '').strip()

        g = 0
        model = None
        m = REQ_GPU_RE.search(gres)
        if m:
            model = normalize_model(m.group(1))
            try: g = int(m.group(2))
            except: g = 0
        else:
            if looks_like_gpu_model(part):
                model = normalize_model(part)
                mg = re.search(r'gpu:(\d+)', gres, re.IGNORECASE)
                if mg:
                    try: g = int(mg.group(1))
                    except: g = 0

        if not model or g <= 0:
            continue

        per_model_user[model][user] += g
        total_used += g
        users_set.add(user)

    return per_model_user, total_used, len(users_set)

# ---------------------- Count total registered accounts (potential GPU users) ----------------------
def total_accounts_candidate_users() -> int:
    for cmd in (
        ['sacctmgr','show','user','format=User','-nP'],
        ['sacctmgr','list','association','format=User','-nP'],
    ):
        try:
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
            users = {u.strip() for u in out.splitlines() if u.strip()}
            if users:
                return len(users)
        except Exception:
            pass
    # fallback: getent
    try:
        out = subprocess.check_output(['getent','passwd'], text=True, stderr=subprocess.DEVNULL)
        cnt = 0
        for ln in out.splitlines():
            try:
                parts = ln.split(':')
                if len(parts) < 7:
                    continue
                uid  = int(parts[2])
                shell= parts[-1].strip()
                if uid >= 1000 and not any(x in shell for x in ('nologin','false')):
                    cnt += 1
            except Exception:
                continue
        return cnt
    except Exception:
        return 0

def cluster_user_tuple(
    per_model_user: Dict[str, Dict[str, int]],
    total_used: int,
    accounts_all: int,
) -> Tuple[int,int,int]:
    user_totals = defaultdict(int)
    for _, d in per_model_user.items():
        for u, g in d.items():
            user_totals[u] += g
    using_users = len(user_totals)
    if total_used <= 0 or using_users == 0:
        return (0, 0, accounts_all)
    target = int(round(0.8 * total_used))
    acc = 0
    k = 0
    for _, g in sorted(user_totals.items(), key=lambda x: x[1], reverse=True):
        acc += g
        k += 1
        if acc >= target:
            break
    return (using_users, k, accounts_all)

# ---------------------- My job counts per model ----------------------
def my_job_counts_by_model(user: Optional[str]) -> Dict[str, Dict[str, int]]:
    """Returns {model: {'R': N, 'PD': N}} for the current user."""
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {'R': 0, 'PD': 0})
    if not user:
        return counts
    try:
        out = subprocess.check_output(
            ['squeue', '-u', user, '-o', '%T|%P|%b', '-h'],
            text=True, stderr=subprocess.DEVNULL
        )
    except Exception:
        return counts
    for ln in out.splitlines():
        if not ln.strip(): continue
        state, part, gres = (ln.split('|') + ['', '', ''])[:3]
        state = state.strip().upper()
        m = REQ_GPU_RE.search(gres or '')
        if m:
            model = normalize_model(m.group(1))
        elif looks_like_gpu_model(part):
            model = normalize_model(part)
        else:
            continue
        if state == 'RUNNING':
            counts[model]['R'] += 1
        elif state == 'PENDING':
            counts[model]['PD'] += 1
    return counts


# ---------------------- Job counts by model ----------------------
def job_counts_by_model() -> Dict[str, Dict[str, int]]:
    """
    Returns: {model: {'R': running_jobs, 'PD': pending_jobs, 'T': total_jobs}}
    """
    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {'R':0,'PD':0,'T':0})
    try:
        out = subprocess.check_output(
            ['squeue','-o','%T|%P|%b','-h'],
            text=True, stderr=subprocess.DEVNULL
        )
    except Exception:
        out = ""

    for ln in out.splitlines():
        if not ln.strip(): continue
        state, part, gres = (ln.split('|') + ['','',''])[:3]
        state = state.strip().upper()
        m = REQ_GPU_RE.search(gres or '')
        if m:
            model = normalize_model(m.group(1))
        else:
            model = normalize_model(part) if looks_like_gpu_model(part) else '(unknown)'
        # count only jobs that look GPU-related (model known)
        if model and model != '(unknown)':
            if state == 'RUNNING': counts[model]['R'] += 1
            elif state == 'PENDING': counts[model]['PD'] += 1
            counts[model]['T'] += 1
    return counts

# ---------------------- Availability color helper ----------------------
def color_avail_text(avail: int, total: int) -> Text:
    pct = (avail / total) if total else 0.0
    if pct >= 0.5: color = "green"
    elif pct >= 0.2: color = "orange1"
    else: color = "red"
    return Text(str(avail), style=f"bold {color}")

# ---------------------- Build usage table ----------------------
def build_usage_table(agg, model_parts, part_weights, user: Optional[str] = None):
    # sort columns by weighted capacity (Total * max part weight)
    weight = {
        m: max((part_weights.get(p, 0.0) for p in parts), default=0.0)
        for m, parts in model_parts.items()
    }
    cols = sorted(
        agg.keys(),
        key=lambda m: agg[m]['Total'] * weight.get(m, 0.0),
        reverse=True
    )

    # cluster totals
    gtot = sum(agg[m]['Total'] for m in cols)
    gusa = sum(agg[m]['Used']  for m in cols)

    # job counts by model (all users)
    jc = job_counts_by_model()

    # my job counts by model
    my_jc = my_job_counts_by_model(user)

    def _col_bg(m):
        """
        Piecewise: dark-green → neutral-charcoal → dark-red.
        Routes through a neutral midpoint to avoid the muddy olive/brown
        that HSV hue rotation produces at 50% usage.
        """
        tot = agg[m]['Total']
        p = agg[m]['Used'] / tot if tot else 0.0
        p = max(0.0, min(1.0, p))
        G = (0x18, 0x30, 0x99)   # bright blue  (all free)
        N = (0x1e, 0x1e, 0x2a)   # dark blue-neutral (half used)
        R = (0x33, 0x18, 0x18)   # dark crimson      (exhausted)
        if p <= 0.5:
            t = p * 2
            c = tuple(int(a + t * (b - a)) for a, b in zip(G, N))
        else:
            t = (p - 0.5) * 2
            c = tuple(int(a + t * (b - a)) for a, b in zip(N, R))
        return f'on #{c[0]:02x}{c[1]:02x}{c[2]:02x}'

    # Build table
    table = Table(title="Cluster Usage")
    table.add_column("")  # row labels
    for m in cols:
        used, tot = agg[m]['Used'], agg[m]['Total']
        pct = (used / tot * 100) if tot else 0.0
        bg = _col_bg(m)
        table.add_column(
            f"{usage_emoji(pct)}{m}⋅{tot}",
            justify="right",
            style=bg,
            header_style=f"bold {bg}".strip(),
        )
    table.add_column(f"Total⋅{gtot}", justify="right")

    # Usage row: left=used (colored fill), right=available (dim number)
    # Column background provides the availability signal — see add_column below
    def make_bar(used: int, tot: int, width=8) -> Text:
        pct = used/tot if tot else 0.0
        if pct <= 0.30:
            bg_color, text_color = "blue", "white"
        elif pct <= 0.70:
            bg_color, text_color = "orange1", "black"
        else:
            bg_color, text_color = "red", "white"

        avail = tot - used
        fw = max(int(round(pct * width)), len(str(used)))
        aw = max(width - fw, len(str(avail)))

        result = Text()
        result.append(f"{round(pct*100)}%⋅", style=bg_color)
        result.append(f"|{used:<{fw}}", style=f"bold {text_color} on {bg_color}")
        result.append(f"{avail:>{aw}}|", style="bright_black")
        return result

    row_pct_colored = [make_bar(agg[m]['Used'], agg[m]['Total']) for m in cols]
    cluster_colored = make_bar(sum(agg[m]['Used'] for m in cols), gtot)

    # Jobs (R/P) row per model + total (all users)
    jobs_row = []
    total_R = total_PD = 0
    for m in cols:
        R = jc.get(m, {}).get('R', 0)
        P = jc.get(m, {}).get('PD', 0)
        jobs_row.append(Text(f"{R}/{P}"))
        total_R += R; total_PD += P
    jobs_total_cell = Text(f"{total_R}/{total_PD}")

    # My jobs (R/P) row per model + total
    my_jobs_row = []
    my_total_R = my_total_PD = 0
    for m in cols:
        R = my_jc.get(m, {}).get('R', 0)
        P = my_jc.get(m, {}).get('PD', 0)
        my_jobs_row.append(Text(f"{R}/{P}") if (R or P) else Text(''))
        my_total_R += R; my_total_PD += P
    my_jobs_total = Text(f"{my_total_R}/{my_total_PD}") if (my_total_R or my_total_PD) else Text('')

    # Add rows
    table.add_row("Availability", *row_pct_colored, cluster_colored)
    table.add_row("Jobs (R/P)",   *jobs_row,      jobs_total_cell)
    table.add_row("My Jobs",      *my_jobs_row,   my_jobs_total)

    return Align(table, align='center')

# ---------------------- "My Jobs" table ----------------------
def _consecutor(id_list: List[int]) -> str:
    if not id_list: return ''
    ids = sorted(int(x) for x in id_list)
    groups = []
    start = prev = ids[0]
    for x in ids[1:]:
        if x == prev + 1:
            prev = x
            continue
        groups.append((start, prev))
        start = prev = x
    groups.append((start, prev))
    return ' '.join([f'{{{a}..{b}}}' if a != b else f'{a}' for a,b in groups])

def _fmt_td(td: timedelta) -> str:
    total = int(td.total_seconds())
    if total < 0: total = 0
    d, r = divmod(total, 86400)
    h, r = divmod(r, 3600)
    m, _ = divmod(r, 60)
    return (f"{d}-" if d else "") + f"{h}:{m:02d}"

def my_jobs_table(user: Optional[str]) -> Optional[Align]:
    if not user:
        try:
            user = subprocess.check_output(['whoami'], text=True).strip()
        except Exception:
            return None

    try:
        out = subprocess.check_output(
            ['squeue','-u',user,'-o','%i|%j|%T|%P|%b|%S','-h'],
            text=True, stderr=subprocess.DEVNULL
        )
    except Exception:
        out = ""

    if not out.strip():
        return None

    jobs: Dict[str, Dict[str, Dict[str, List[Tuple[int,int,str]]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for ln in out.splitlines():
        if not ln.strip(): continue
        jid_s, jname, state, part, gres, s_eta = (ln.split('|') + ['','','','',''])[:6]
        try:
            jid = int(jid_s)
        except Exception:
            continue

        m = REQ_GPU_RE.search(gres or '')
        if m:
            model = normalize_model(m.group(1))
            try: g = int(m.group(2))
            except: g = 0
        else:
            if looks_like_gpu_model(part or ''):
                model = normalize_model(part)
                mg = re.search(r'gpu:(\d+)', gres or '', re.IGNORECASE)
                if mg:
                    try: g = int(mg.group(1))
                    except: g = 0
                else:
                    g = 0
            else:
                model = '(unknown)'; g = 0

        start_in = ''
        if state.upper() == 'PENDING':
            eta_dt = None
            if s_eta and s_eta != 'N/A':
                try:
                    eta_dt = datetime.strptime(s_eta.replace('T',' ').split('.')[0], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    eta_dt = None
            if eta_dt is None:
                eta_dt = predict_pending_job_eta(jid, details=False)
            if isinstance(eta_dt, tuple):
                eta_dt = eta_dt[0]
            if isinstance(eta_dt, datetime):
                start_in = _fmt_td(eta_dt - datetime.now())
            else:
                start_in = '?'

        jobs[jname][model][state.upper()].append((jid, g, start_in))

    table = Table(title=f"{user}'s Jobs")
    for c in ['Status', 'Job', 'GPU', '#', 'IDs', 'Start In']:
        table.add_column(c)

    STATUS_ORDER = ['RUNNING', 'PENDING']
    for status in STATUS_ORDER:
        names_with_status = [jn for jn, per_model in jobs.items() if any(per_model[m].get(status) for m in per_model)]
        for jn in names_with_status:
            per_model = jobs[jn]
            models = sorted(per_model.keys())
            first_row = True
            for mod in models:
                triplets = per_model[mod].get(status, [])
                if not triplets:
                    continue
                ids = [jid for (jid, _g, _si) in triplets]
                total_g = sum(_g for (_jid, _g, _si) in triplets if isinstance(_g, int))
                si_vals = [si for (_jid, _g, si) in triplets if si]
                si_text = si_vals[0] if si_vals else ('' if status == 'RUNNING' else '?')
                table.add_row(
                    status if first_row else '',
                    jn if first_row else '',
                    mod,
                    str(total_g),
                    _consecutor(ids),
                    si_text,
                )
                first_row = False

    return Align(table, align='center')

# ---------------------- Compose render ----------------------
def render_once():
    rows = parse_sinfo_rows()
    agg, model_parts = aggregate(rows)
    part_weights = get_partition_weights()
    try:
        me = args.user or subprocess.check_output(['whoami'], text=True).strip()
    except Exception:
        me = None
    usage = build_usage_table(agg, model_parts, part_weights, user=me)
    jobs_block = my_jobs_table(me)

    parts = [usage]
    if args.nodes:
        node_block = build_node_table(rows)
        if node_block:
            parts += [Text(''), node_block]
    if jobs_block:
        parts += [Text(''), jobs_block]
    return Group(*parts) if len(parts) > 1 else parts[0]

# ---------------------- Entrypoint ----------------------
def main():
    if not args.live:
        print(render_once())
    else:
        with Live(refresh_per_second=1) as live:
            while True:
                live.update(render_once())
                time.sleep(1)

if __name__ == '__main__':
    main()