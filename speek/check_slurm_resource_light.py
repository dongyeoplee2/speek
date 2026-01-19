#!/usr/bin/env python3
import subprocess, re, argparse, time, os
from collections import defaultdict
from typing import Optional, Dict, Set, Tuple, List
from datetime import datetime, timedelta

from rich import print
from rich.table import Table
from rich.align import Align
from rich.text import Text
from rich.live import Live
from rich.console import Group

# --- a-priori predictors (from slurm_predict.py) ---
try:
    from slurm_predict import (
        predict_max_nonpending_gpus,
        predict_pending_job_eta,
    )
except Exception:
    def predict_max_nonpending_gpus(gpu: str, when_str: str) -> int:
        return 0
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
    '--sbatch', type=str, default=None,
    help='(unused by predictor now) Path to an sbatch script; still parsed for display hints.'
)
parser.add_argument(
    '-u','--user', type=str, default=None,
    help='User to show jobs for (default: current user).'
)
parser.set_defaults(live=False)

args = parser.parse_args()

# ---------------------- Regex ----------------------
GPU_TOTAL_RE = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)
GPU_USED_RE  = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)
REQ_GPU_RE   = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)
SB_GRES_RE   = re.compile(r'--gres=.*?gpu(?::([A-Za-z0-9\-]+))?(?::(\d+))?', re.IGNORECASE)

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

def parse_sbatch_file(path: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    if not path or not os.path.isfile(path):
        return (None, None, None, None)
    part = qos = None
    g_model = None
    g_count = None
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        for raw in f:
            line = raw.strip()
            if not line.startswith('#SBATCH'):
                continue
            m = re.search(r'(?:-p|--partition)\s+([^\s#]+)', line) or re.search(r'--partition=([^\s#]+)', line)
            if m: part = m.group(1).strip()
            m = re.search(r'--qos\s+([^\s#]+)', line) or re.search(r'--qos=([^\s#]+)', line)
            if m: qos = m.group(1).strip()
            m = SB_GRES_RE.search(line)
            if m:
                if m.group(1): g_model = normalize_model(m.group(1))
                if m.group(2):
                    try: g_count = int(m.group(2))
                    except: pass
    return (part, qos, g_model, g_count)

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
def build_usage_table(agg, model_parts, part_weights):
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

    # users (RUNNING) — model-aware from squeue
    per_model_user, total_used, _using_accounts = running_user_gpu_usage_model_aware()
    accounts_all = total_accounts_candidate_users()
    using_users, top80_users, accounts_num = cluster_user_tuple(per_model_user, total_used, accounts_all)

    # job counts by model
    jc = job_counts_by_model()

    # optional sbatch guidance (not used by predictor, but kept)
    sb_part, sb_qos, sb_gmodel, sb_gcount = parse_sbatch_file(args.sbatch)

    # Build table
    table = Table(title="Cluster Usage")
    table.add_column("")  # row labels
    for m in cols:
        used, tot = agg[m]['Used'], agg[m]['Total']
        pct = (used / tot * 100) if tot else 0.0
        table.add_column(f"{usage_emoji(pct)}{m}⋅{tot}", justify="right")
    table.add_column(f"Total⋅{gtot}", justify="right")

    # Usage row: colored progress bars (show % and fill/empty counts)
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
        result.append(f"|{used:<{fw}}", style="bright_black")
        result.append(f"{avail:>{aw}}", style=f"bold {text_color} on {bg_color}")
        result.append("|", style="bright_black")
        return result

    row_pct_colored = [make_bar(agg[m]['Used'], agg[m]['Total']) for m in cols]
    cluster_colored = make_bar(sum(agg[m]['Used'] for m in cols), gtot)

    # Users row (per-GPU = distinct running users on that model; Total = using/top80/accounts_all)
    row_users = [Text(str(len(per_model_user.get(m, {})))) for m in cols]
    total_users_cell = Text(f"{using_users}/{top80_users}/{accounts_num}")

    # Will it pend? (a-priori predictor from slurm_predict.py)
    predict_row   = []
    max_ok_row    = []  # NEW: maximum requestable GPUs without pending (per model)
    max_ok_total  = 0
    for m in cols:
        avail_model = max(0, agg[m]['Total'] - agg[m]['Used'])
        min_req = 3
        max_req = max(min_req, int(round(0.8 * avail_model)))
        max_req = min(max_req, max(min_req, avail_model))

        model_for_pred = sb_gmodel or m
        try:
            max_nonpending = int(predict_max_nonpending_gpus(model_for_pred, "now"))
        except Exception:
            max_nonpending = 0

        # traffic light based on min/max request sizes
        small_ok = (max_nonpending >= min_req)
        large_ok = (max_nonpending >= max_req)

        if small_ok and large_ok:
            color = "green";  symbol = "✔"
        elif small_ok:
            color = "orange1"; symbol = "⚠"
        else:
            color = "red";    symbol = "✘"

        cell = Text(f"({min_req}/{max_req}) ") + Text(symbol + " ", style=color)
        predict_row.append(cell)

        # NEW row: max no-pend number
        max_ok_row.append(Text(str(max_nonpending)))
        max_ok_total += max_nonpending

    # NEW: Jobs (R/P/T) row per model + total
    jobs_row = []
    total_R = total_PD = total_T = 0
    for m in cols:
        R = jc.get(m, {}).get('R', 0)
        P = jc.get(m, {}).get('PD', 0)
        T = jc.get(m, {}).get('T', 0)
        jobs_row.append(Text(f"{R}/{P}/{T}"))
        total_R += R; total_PD += P; total_T += T
    jobs_total_cell = Text(f"{total_R}/{total_PD}/{total_T}")

    # Add rows
    table.add_row("Availability", *row_pct_colored, cluster_colored)
    table.add_row("Will it pend", *predict_row, "")
    table.add_row("Max no-pend",  *max_ok_row,    Text(str(max_ok_total)))   # NEW
    table.add_row("Jobs (R/P/T)", *jobs_row,      jobs_total_cell)           # NEW
    table.add_row("Users",        *row_users,     total_users_cell)

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
    usage = build_usage_table(agg, model_parts, part_weights)

    try:
        me = args.user or subprocess.check_output(['whoami'], text=True).strip()
    except Exception:
        me = None
    jobs_block = my_jobs_table(me)

    if jobs_block:
        return Group(usage, Text("\n"), jobs_block)
    else:
        return usage

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