# slurm_predictor.py
import subprocess, re, time
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Optional, Tuple, List

# =========================
# TTL cache for shell calls
# =========================
_CMD_CACHE: Dict[Tuple[str, ...], Tuple[float, str]] = {}
DEFAULT_TTL = {
    "sinfo": 10.0,        # topology changes fast-ish
    "squeue_PD": 5.0,     # pending set used often
    "squeue_start": 20.0, # start times ok to cache a bit
    "squeue_R": 5.0,      # running snapshot
    "sprio_PD": 15.0,     # PD priority sample
    "sshare": 30.0,       # fairshare changes slowly
    "sacctmgr": 300.0,    # limits change rarely
    "whoami": 300.0,
}

def _run(cmd: List[str], tag: str, ttl: Optional[float] = None) -> str:
    """Run a command with small TTL cache."""
    ttl = DEFAULT_TTL.get(tag, 5.0) if ttl is None else ttl
    key = tuple(cmd)
    now = time.time()
    if key in _CMD_CACHE:
        ts, out = _CMD_CACHE[key]
        if now - ts <= ttl:
            return out
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        out = ""
    _CMD_CACHE[key] = (now, out)
    return out

# =========================
# Regex & small parsers
# =========================
_GPU_TOK_RE   = re.compile(r'gpu(?::([A-Za-z0-9\-]+))?(?::(\d+))?', re.IGNORECASE)
_GPU_TOTAL_RE = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)
_GPU_USED_RE  = re.compile(r'gpu:([A-Za-z0-9\-]+):(\d+)', re.IGNORECASE)

def _normalize_model(m: Optional[str]) -> Optional[str]:
    if not m:
        return None
    m = m.strip()
    if m.startswith('A100-SXM4-80GB'): return 'A100-80GB'
    if m.startswith('A100-SXM4-40GB'): return '4A100'
    if m.startswith('A100-PCIE-40GB'): return 'A100-40GB'
    if m.upper().startswith('RTX2080TI'): return '2080ti'
    if m == 'RTX3090': return '3090'
    return m

def _parse_when(when_str: str) -> datetime:
    s = (when_str or "").strip().lower()
    if s in ("", "now", "right now", "immediately"):
        return datetime.now()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y/%m/%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(when_str, fmt)
        except Exception:
            pass
    return datetime.now()

def _parse_hms_or_d_hms(s: str) -> Optional[timedelta]:
    """Parse Slurm time like '1-02:03:04' or '02:03:04' into timedelta."""
    if not s or s.upper() in ("N/A", "UNLIMITED"):
        return None
    try:
        if '-' in s:
            d, hms = s.split('-', 1)
            h, m, sec = hms.split(':')
            return timedelta(days=int(d), hours=int(h), minutes=int(m), seconds=int(sec))
        else:
            h, m, sec = s.split(':')
            return timedelta(hours=int(h), minutes=int(m), seconds=int(sec))
    except Exception:
        return None

# =======================================
# Snapshots: free GPUs, partitions, PD
# =======================================
def _snapshot_free_by_node_partition_model() -> Tuple[
    Dict[str, Dict[str, Dict[str, int]]],
    Dict[str, Dict[str, int]]
]:
    """
    Returns:
      free_by_node[partition][node][model] = free gpus at node
      free_by_part_model[partition][model] = sum free across nodes
    """
    out = _run(['sinfo','-N','-O','NodeHost,Partition,Gres,GresUsed'], tag="sinfo")
    free_by_node: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
    free_by_part_model: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    lines = [ln.rstrip() for ln in out.splitlines() if ln.strip()]
    if not lines:
        return free_by_node, free_by_part_model

    for ln in lines[1:]:
        s2 = re.sub(r'(gpu:)', r'  \1', ln).strip()
        parts = re.split(r'\s{2,}', s2)
        if len(parts) == 4:
            node, part, gres, gres_used = parts
        elif len(parts) >= 3:
            node, gres, gres_used = parts[:3]
            part = "(unknown)"
        else:
            continue

        mt = _GPU_TOTAL_RE.search(gres or "")
        mu = _GPU_USED_RE.search(gres_used or "")
        if not mt:
            continue
        raw_model = mt.group(1)
        total = int(mt.group(2))
        used = 0
        if mu and _normalize_model(mu.group(1)) == _normalize_model(raw_model):
            try:
                used = int(mu.group(2))
            except Exception:
                used = 0

        model = _normalize_model(raw_model) or raw_model
        free = max(0, total - used)
        P = (part or "(unknown)").strip()
        N = (node or "").strip()
        free_by_node[P][N][model] = max(free_by_node[P][N].get(model, 0), free)
        free_by_part_model[P][model] += free

    return free_by_node, free_by_part_model

def _eligible_partitions_for_model(model: str,
                                   free_by_part_model: Dict[str, Dict[str, int]]) -> List[str]:
    tnorm = _normalize_model(model) or model
    parts = []
    for P, d in free_by_part_model.items():
        for mod in d.keys():
            if (_normalize_model(mod) or mod) == tnorm:
                parts.append(P)
                break
    return parts

def _snapshot_pending_demand_by_part_model(target_model: str,
                                           parts_scope: Optional[List[str]],
                                           strict_model: bool) -> int:
    """
    Sum pending GPU demand for target_model across partitions in parts_scope.
    strict_model=False counts generic 'gpu:<N>' as competing demand.
    """
    out = _run(['squeue','-t','PD','-o','%P|%b','-h'], tag="squeue_PD")
    if not out:
        return 0
    tnorm = _normalize_model(target_model) or target_model
    parts_allow = set(parts_scope) if parts_scope else None
    demand = 0

    for ln in out.splitlines():
        if not ln.strip(): continue
        P, b = (ln.split('|') + ['',''])[:2]
        P = P.strip()
        if parts_allow is not None and P not in parts_allow:
            continue
        m = _GPU_TOK_RE.search(b or "")
        if not m: 
            continue
        mod = _normalize_model(m.group(1)) if m.group(1) else None
        cnt = int(m.group(2) or 0)
        if cnt <= 0:
            continue
        if mod is None:
            if not strict_model:
                demand += cnt
        else:
            if mod == tnorm:
                demand += cnt
    return demand

def _running_releases_in_partition(
    partition: str,
    model: Optional[str]
) -> List[timedelta]:
    """
    Build a multiset of GPU releases (each GPU = one element with a remaining time).
    Source: squeue -t R -p PART -o "%L|%b"
    Only counts GPUs that match 'model' (or all if model is None).
    """
    out = _run(['squeue','-t','R','-p',partition,'-o','%L|%b','-h'], tag="squeue_R")
    releases: List[timedelta] = []
    tnorm = _normalize_model(model) if model else None

    for ln in out.splitlines():
        if not ln.strip(): continue
        L, b = (ln.split('|') + ['',''])[:2]
        td = _parse_hms_or_d_hms(L.strip())
        if td is None:
            continue
        m = _GPU_TOK_RE.search(b or "")
        if not m:
            continue
        mod = _normalize_model(m.group(1)) if m.group(1) else None
        cnt = int(m.group(2) or 0)
        if cnt <= 0:
            continue
        if tnorm is None or mod == tnorm:
            # each GPU contributes one "release at td"
            releases.extend([td] * cnt)
    releases.sort(key=lambda x: x.total_seconds())
    return releases

# ===================================================
# Auto-tuned proxies from cheap probes (adaptive)
# ===================================================
def _auto_penalty_reservation(parts: List[str]) -> float:
    """
    Heuristic: increase penalty when many PD jobs have near-term start times.
    """
    if not parts:
        return 0.15
    out = _run(['squeue','--start','-t','PD','-o','%P|%S','-h'], tag="squeue_start")
    soon = 0; total = 0
    now = datetime.now()
    for ln in out.splitlines():
        if not ln.strip(): continue
        P, S = (ln.split('|') + ['',''])[:2]
        if P not in parts: 
            continue
        total += 1
        # Parse known formats; N/A or blank means unknown
        try:
            if S and S != "N/A":
                # Slurm usually prints like "2025-09-30T12:34:56"
                S2 = S.replace('T',' ')
                eta = datetime.strptime(S2.split('.')[0], "%Y-%m-%d %H:%M:%S")
                # "soon" = within 30 minutes
                if (eta - now).total_seconds() <= 30*60:
                    soon += 1
        except Exception:
            pass
    if total == 0:
        return 0.15
    ratio = soon / total
    # Map ratio->[0.05..0.4]
    return round(0.05 + 0.35 * min(1.0, max(0.0, ratio)), 2)

def _auto_generic_vs_model_PD(parts: List[str]) -> bool:
    """
    Return strict_model flag:
      True  -> count only explicit model matches
      False -> generic gpu:<N> also competes
    We set strict=False if most PD is generic in these partitions.
    """
    out = _run(['squeue','-t','PD','-o','%P|%b','-h'], tag="squeue_PD")
    if not out:
        return False
    total = 0; explicit = 0
    for ln in out.splitlines():
        if not ln.strip(): continue
        P, b = (ln.split('|') + ['',''])[:2]
        if parts and P not in parts:
            continue
        m = _GPU_TOK_RE.search(b or "")
        if not m:
            continue
        total += 1
        if m.group(1):  # model present
            explicit += 1
    if total == 0:
        return False
    # strict if majority explicitly specify model
    return explicit / total >= 0.6

def _auto_priority_discount(parts: List[str], my_user: str) -> float:
    """
    Compare my fairshare vs PD jobs' fairshare and discount effective free if I'm weaker.
    Returns a discount in [0.0..0.3].
    """
    # My fairshare snapshot (best-effort)
    my_fs = None
    out_fs = _run(['sshare','-al'], tag="sshare")
    for ln in out_fs.splitlines():
        # crude match: first column often user; sites vary
        if ln.startswith(my_user + " "):
            # Try to find 'FairShare=' style tokens; fallback to last number on the line
            m = re.search(r'FairShare\s*=\s*([0-9.]+)', ln)
            if m:
                try: my_fs = float(m.group(1)); break
                except: pass
            m2 = re.findall(r'([0-9.]+)', ln)
            if m2:
                try: my_fs = float(m2[-1]); break
                except: pass

    # PD fairshare sample from sprio (fairshare component is %f)
    fs_vals: List[float] = []
    out_pr = _run(['sprio','-o','%P|%f','-h'], tag="sprio_PD")
    for ln in out_pr.splitlines():
        if not ln.strip(): continue
        P, fs = (ln.split('|') + ['',''])[:2]
        if parts and P not in parts:
            continue
        try:
            v = float(fs)
            if v >= 0:
                fs_vals.append(v)
        except Exception:
            continue

    if my_fs is None or not fs_vals:
        return 0.0

    fs_vals.sort()
    median = fs_vals[len(fs_vals)//2]
    if my_fs >= median:
        return 0.0
    # Map how far below median to a discount up to 0.3
    gap = median - my_fs
    scale = median if median > 0 else 1.0
    frac = max(0.0, min(1.0, gap / scale))
    return round(0.3 * frac, 2)

# ==================================
# Hard caps (best-effort remaining)
# ==================================
_LIMIT_RE = re.compile(r'(?:gpu(?::[A-Za-z0-9\-]+)?)\s*=?\s*([0-9]+)', re.IGNORECASE)

def _caps_ceiling_for_model(user: Optional[str], model: str) -> Optional[int]:
    """
    Try to infer a ceiling (MaxTRESPU/GrpTRES ...) for gpu or gpu:MODEL from sacctmgr.
    Returns None if unknown.
    """
    qos = _run(['sacctmgr','show','qos','format=Name,MaxTRESPU,MaxTRES,GrpTRES','-Pn'], tag="sacctmgr")
    assoc = _run(['sacctmgr','show','assoc','format=User,Account,MaxTRESPU,GrpTRES','-Pn'], tag="sacctmgr")
    tnorm = _normalize_model(model) or model
    caps: List[int] = []

    def scan_field(s: str):
        for tok in (s or "").split(','):
            t = tok.strip()
            if not t:
                continue
            # If specific model is present in token and doesn't match, skip it
            if 'gpu:' in t.lower():
                # t like 'gpu:A100=4' or 'gres/gpu:A100=4'
                parts = t.split(':', 2)
                if len(parts) >= 2:
                    mod = parts[1].split('=')[0]
                    if (_normalize_model(mod) or mod) != tnorm:
                        continue
            if 'gpu' in t.lower():
                m = _LIMIT_RE.search(t)
                if m:
                    try: caps.append(int(m.group(1)))
                    except: pass

    for ln in qos.splitlines():
        _, maxpu, maxt, grpt = (ln.split('|') + ['','',''])[:4]
        for fld in (maxpu, maxt, grpt):
            scan_field(fld or "")

    if user:
        for ln in assoc.splitlines():
            U, _, maxpu, grpt = (ln.split('|') + ['','','',''])[:4]
            if U != user: 
                continue
            for fld in (maxpu, grpt):
                scan_field(fld or "")

    return max(caps) if caps else None

def _live_gpu_in_use_by_user(user: Optional[str]) -> int:
    """
    Quick live count of GPUs user is running now (any model), to adjust caps.
    """
    if not user:
        return 0
    out = _run(['squeue','-t','R','-o','%u|%b','-h'], tag="squeue_R")
    total = 0
    for ln in out.splitlines():
        if not ln.strip(): continue
        u, b = (ln.split('|') + ['',''])[:2]
        if u != user:
            continue
        m = _GPU_TOK_RE.search(b or "")
        if not m:
            continue
        try:
            cnt = int(m.group(2) or 0)
        except Exception:
            cnt = 0
        total += cnt
    return total

# ===========================================
# Core: node fit + contention + auto-proxies
# ===========================================
def _best_fit_and_effective_free(model: str) -> Tuple[int, int, List[str]]:
    """
    Returns:
      node_max_fit_now: max #GPUs of 'model' available on any single node right now
      effective_free:   (free_sum - pending_sum) with adaptive penalties/discounts applied
      eligible_parts:   which partitions expose this model in sinfo
    """
    free_by_node, free_by_part_model = _snapshot_free_by_node_partition_model()
    parts = _eligible_partitions_for_model(model, free_by_part_model)

    # Node-level fit (must place on one node)
    tnorm = _normalize_model(model) or model
    node_max = 0
    for P in parts:
        for node, models in free_by_node.get(P, {}).items():
            node_max = max(node_max, models.get(tnorm, 0))

    # Partition-level free sum
    free_sum = sum(free_by_part_model.get(P, {}).get(tnorm, 0) for P in parts)

    # Adaptive strictness for parsing PD demand
    strict_model = _auto_generic_vs_model_PD(parts)
    demand_sum = _snapshot_pending_demand_by_part_model(tnorm, parts, strict_model=strict_model)

    # Adaptive reservation penalty
    penalty_frac = _auto_penalty_reservation(parts)
    penalty = int(round(free_sum * penalty_frac))

    effective = max(0, free_sum - demand_sum - penalty)

    # Adaptive priority discount vs current PD set
    user = _run(['whoami'], tag="whoami").strip() or None
    prio_disc = _auto_priority_discount(parts, user)
    effective = int(max(0, round(effective * (1.0 - prio_disc))))

    return node_max, effective, parts

# ==========================
# Public predictor functions
# ==========================
def predict_max_nonpending_gpus(gpu: str, when_str: str) -> int:
    """
    Best-effort max N for 'gpu' that likely won't pend now:
      min( node_max_fit_now, effective_free_after_penalties, remaining_cap_if_known )
    'when_str' is parsed for future extension; current model uses snapshots 'now'.
    """
    _ = _parse_when(when_str)
    node_max, effective, _parts = _best_fit_and_effective_free(gpu)

    user = _run(['whoami'], tag="whoami").strip() or None
    cap = _caps_ceiling_for_model(user, gpu)
    if cap is not None:
        # crude remaining = cap - live GPUs in use (any model)
        live = _live_gpu_in_use_by_user(user)
        cap = max(0, cap - live)

    max_ok = node_max if cap is None else min(node_max, cap)
    max_ok = min(max_ok, effective)
    return max(0, int(max_ok))

def predict_will_pend(gpu: str, n: int, when_str: str) -> bool:
    """
    True if requesting 'n' GPUs of type 'gpu' will likely PEND now.
    """
    if n <= 0:
        return False
    return n > predict_max_nonpending_gpus(gpu, when_str)

# ==========================
# Job-centric ETA helpers
# ==========================
def _job_info(jobid: int) -> dict:
    """
    Lightweight snapshot for one job:
    - Partition, GRES request, Slurm ETA (if any), State, Reason, Priority
    """
    info = {
        "jobid": jobid, "partition": None, "gres": None, "eta": None,
        "state": None, "reason": None, "priority": None, "model": None, "count": 0
    }
    # squeue: partition/gres/start/state/reason
    out = _run(['squeue','-j',str(jobid),'-o','%i|%P|%b|%S|%T|%R','-h'], tag="squeue_start")
    if out:
        # Expect a single line for this job
        ln = out.splitlines()[0].strip()
        jid, P, b, S, T, R = (ln.split('|') + ['','','','',''])[:6]
        info["partition"] = P.strip() or None
        info["gres"] = b.strip() or None
        info["state"] = T.strip() or None
        info["reason"] = R.strip() or None
        if S and S != "N/A":
            try:
                info["eta"] = datetime.strptime(S.replace('T',' ').split('.')[0], "%Y-%m-%d %H:%M:%S")
            except Exception:
                info["eta"] = None

    # sprio: numeric priority for ordering (optional)
    outp = _run(['sprio','-j',str(jobid),'-o','%i|%Y','-h'], tag="sprio_PD")
    if outp:
        try:
            _, pr = (outp.splitlines()[0].split('|') + [''])[:2]
            info["priority"] = float(pr)
        except Exception:
            pass

    # Extract model/count from %b (or fallback to scontrol)
    if info["gres"]:
        m = _GPU_TOK_RE.search(info["gres"])
        if m:
            mod = _normalize_model(m.group(1)) if m.group(1) else None
            cnt = int(m.group(2) or 0)
            info["model"] = mod
            info["count"] = cnt
    if not info["model"] or info["count"] <= 0:
        # Fallback: scontrol show job for TresPerNode|Gres
        sc = _run(['scontrol','show','job',str(jobid)], tag="sinfo")
        mm = _GPU_TOK_RE.search(sc or "")
        if mm:
            mod = _normalize_model(mm.group(1)) if mm.group(1) else None
            try:
                cnt = int(mm.group(2) or 0)
            except Exception:
                cnt = 0
            if mod: info["model"] = mod
            if cnt > 0: info["count"] = cnt

    return info

def _pending_ahead_demand(
    partition: str,
    model: Optional[str],
    my_priority: Optional[float]
) -> int:
    """
    Sum pending GPU counts in the same partition (and model/generic)
    with STRICTNESS tuned by _auto_generic_vs_model_PD, but **only for jobs ahead of us**.
    """
    out = _run(['squeue','-t','PD','-o','%i|%P|%b','-h'], tag="squeue_PD")
    if not out:
        return 0
    strict = _auto_generic_vs_model_PD([partition])
    # We'll need priorities for those jobs:
    # Build a quick map jobid->priority
    spr = _run(['sprio','-o','%i|%Y','-h'], tag="sprio_PD")
    prio_map = {}
    for ln in spr.splitlines():
        if not ln.strip(): continue
        jid, pr = (ln.split('|') + ['',''])[:2]
        try:
            prio_map[int(jid)] = float(pr)
        except Exception:
            continue

    tnorm = _normalize_model(model) if model else None
    demand = 0
    for ln in out.splitlines():
        if not ln.strip(): continue
        jid_s, P, b = (ln.split('|') + ['','',''])[:3]
        if P != partition:
            continue
        try:
            jid = int(jid_s)
        except Exception:
            continue
        # Only count jobs strictly ahead of us
        if my_priority is not None and prio_map.get(jid) is not None:
            if prio_map[jid] <= my_priority:
                continue

        m = _GPU_TOK_RE.search(b or "")
        if not m:
            continue
        mod = _normalize_model(m.group(1)) if m.group(1) else None
        cnt = int(m.group(2) or 0)
        if cnt <= 0:
            continue
        if mod is None:
            if not strict:
                demand += cnt
        else:
            if tnorm is None or mod == tnorm:
                demand += cnt
    return demand

# ==========================
# Pending ETA (public)
# ==========================
def predict_pending_job_eta(
    jobid: int,
    details: bool = False
):
    """
    Best-effort ETA for a *pending* job.
    Returns:
      - eta: datetime or None
      - info (optional): dict with simple, non-jargony fields describing the decision.
    Logic:
      1) If Slurm already provides an ETA (%S via 'squeue --start'), use it.
      2) Else:
         - get (partition, model, count)
         - free_now = sum free GPUs for model in partition
         - ahead = sum of GPU demand in PD with higher priority in same partition/model
         - need = max(0, count - free_now + ahead)
         - build release list from running jobs in that partition/model (%L)
         - ETA = now + time of the 'need'-th release (if exists), else None
      3) Apply a small reservation penalty using _auto_penalty_reservation([partition]).
    """
    info = _job_info(jobid)
    now = datetime.now()

    # If not pending, return immediately
    if info["state"] and info["state"] != "PENDING":
        return (now, info) if details else now

    # If Slurm already gives an ETA, prefer that
    if info["eta"]:
        return (info["eta"], info) if details else info["eta"]

    P = info["partition"]
    mod = info["model"]
    cnt = info["count"] or 0
    if not P or cnt <= 0:
        return (None, info) if details else None

    # Current free summary (use your cached snapshot functions)
    free_by_node, free_by_part_model = _snapshot_free_by_node_partition_model()
    tnorm = _normalize_model(mod) if mod else None
    free_now = free_by_part_model.get(P, {}).get(tnorm or "", 0)
    # If model key isn't present exactly as normalized string, try any that normalizes equal
    if free_now == 0 and P in free_by_part_model:
        for mk, val in free_by_part_model[P].items():
            if (_normalize_model(mk) or mk) == (tnorm or mk):
                free_now = val
                break

    # Pending demand ahead of us
    ahead = _pending_ahead_demand(P, mod, info.get("priority"))

    # Reservation/backfill penalty (adaptive)
    penalty_frac = _auto_penalty_reservation([P])
    penalty = int(round((free_now + ahead) * penalty_frac))

    need = max(0, cnt - free_now + ahead + penalty)

    # If we don't "need" additional GPUs beyond free_now, ETA ~ now
    if need <= 0:
        eta = now
        if details:
            info.update({
                "computed_eta": eta,
                "partition": P, "model": mod, "count": cnt,
                "free_now": free_now, "ahead_demand": ahead,
                "reservation_penalty": penalty,
                "need_after_penalty": 0,
                "explanation": "Enough GPUs appear free now (after penalty), so start is likely immediate."
            })
            return eta, info
        return eta

    # Build release list from running jobs in P for this model
    releases = _running_releases_in_partition(P, mod)
    if not releases:
        if details:
            info.update({
                "computed_eta": None,
                "partition": P, "model": mod, "count": cnt,
                "free_now": free_now, "ahead_demand": ahead,
                "reservation_penalty": penalty,
                "need_after_penalty": need,
                "explanation": "No visible running jobs releasing this model in the partition; ETA unknown."
            })
            return None, info
        return None

    # Accumulate earliest releases until we cover the 'need'
    acc = 0
    kth_td = None
    for td in releases:
        acc += 1
        if acc >= need:
            kth_td = td
            break

    if kth_td is None:
        if details:
            info.update({
                "computed_eta": None,
                "partition": P, "model": mod, "count": cnt,
                "free_now": free_now, "ahead_demand": ahead,
                "reservation_penalty": penalty,
                "need_after_penalty": need,
                "explanation": "Even after all visible releases, demand ahead + your request exceeds supply."
            })
            return None, info
        return None

    eta = now + kth_td
    if details:
        info.update({
            "computed_eta": eta,
            "partition": P, "model": mod, "count": cnt,
            "free_now": free_now, "ahead_demand": ahead,
            "reservation_penalty": penalty,
            "need_after_penalty": need,
            "used_releases_count": need,
            "explanation": "ETA estimated from the earliest set of running jobs that free enough GPUs ahead of you."
        })
        return eta, info
    return eta