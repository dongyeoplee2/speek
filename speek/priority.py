# ================== Pending Explanation Helpers ==================

import subprocess
import re
import os
import argparse
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns

# Minimal parser setup
parser = argparse.ArgumentParser(description="SLURM Job Priority Explanation Tool")
parser.add_argument(
    '--explain', type=str, default=None,
    help='Explain why JOBID is pending (priority, limits, resources).'
)
parser.add_argument(
    '--explain-latest', action='store_true',
    help='Explain most recent pending job for current user.'
)

_console = Console()

def _run(cmd):
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return ""

def _current_user():
    out = _run(['whoami']).strip()
    return out or os.environ.get('USER','')

def _find_latest_pending_jobid_for_user(user: str) -> Optional[str]:
    out = _run(['squeue','-u',user,'-t','PD','-o','%i|%V','-h'])
    latest_id, latest_submit = None, None
    for ln in out.splitlines():
        jid, submitted = (ln.split('|')+['',''])[:2]
        if not jid: 
            continue
        # Keep the last row (squeue is often time-sorted) or compare strings
        latest_id = jid
        latest_submit = submitted
    return latest_id

def _squeue_details(jobid: str) -> dict:
    # %R reason, %T state, %t compact state, %P partition, %Q qos, %u user,
    # %C alloc cpus, %b gres, %D nodes, %l time limit, %V submit time
    fmt = '%i|%t|%T|%R|%P|%Q|%u|%b|%C|%D|%l|%V'
    out = _run(['squeue','-j',jobid,'-o',fmt,'-h']).strip()
    keys = ['JobID','t','State','Reason','Partition','QoS','User','GRES','CPUs','Nodes','TimeLimit','Submitted']
    vals = (out.split('|')+['']*len(keys))[:len(keys)] if out else ['']*len(keys)
    return dict(zip(keys, vals))

def _sprio(jobid: str) -> dict:
    """
    Parse `sprio -j` into a dict like {'PRIORITY':..., 'AGE':..., 'FAIRSHARE':..., ...}
    """
    out = _run(['sprio','-j',jobid]).strip()
    # sprio headers vary; we’ll parse by whitespace columns
    lines = [ln for ln in out.splitlines() if ln.strip()]
    if len(lines) < 2:
        return {}
    hdr = re.split(r'\s+', lines[0].strip())
    dat = re.split(r'\s+', lines[1].strip())
    if len(dat) < len(hdr):
        dat += ['']*(len(hdr)-len(dat))
    return dict(zip(hdr, dat))

def _scontrol_job(jobid: str) -> dict:
    """
    Parse `scontrol show job` into key=value dict (best effort).
    """
    out = _run(['scontrol','show','job',jobid]).replace('\n',' ')
    info = {}
    for tok in re.split(r'\s+', out):
        if '=' in tok:
            k,v = tok.split('=',1)
            info[k] = v
    return info

def _sshare_for(user: str) -> dict:
    """
    Get fairshare snapshot for user (one line; cluster aggregates may vary).
    """
    out = _run(['sshare','-al'])
    # Look for a line starting with user (or containing user in the User column)
    # We’ll just pick the first match
    if not out:
        return {}
    header = None
    for ln in out.splitlines():
        if not ln.strip():
            continue
        if re.search(r'\bUser\b', ln) and header is None:
            header = re.split(r'\s+', ln.strip())
            continue
        if header is None:
            continue
        if ln.strip().startswith(user + ' ') or re.search(rf'\b{re.escape(user)}\b', ln):
            cols = re.split(r'\s+', ln.strip())
            cols += ['']*(len(header)-len(cols))
            return dict(zip(header, cols))
    return {}

def _qos_limits(qos: str) -> dict:
    if not qos:
        return {}
    out = _run(['sacctmgr','show','qos',f'name={qos}','format=Name,Priority,MaxTRESPU,MaxTRES,GrpTRES,MaxJobs,MaxSubmitJobs,Preempt','-Pn'])
    if not out:
        return {}
    vals = out.splitlines()[0].split('|')
    keys = ['Name','Priority','MaxTRESPU','MaxTRES','GrpTRES','MaxJobs','MaxSubmitJobs','Preempt']
    vals += ['']*(len(keys)-len(vals))
    return dict(zip(keys, vals))

def _assoc_limits(user: str, account: Optional[str]) -> dict:
    # association limits for (user,account) if known; else just for user
    cmd = ['sacctmgr','show','assoc','-Pn','format=User,Account,DefQOS,MaxTRESPU,GrpTRES,GrpJobs,GrpTRESMins,GrpWall']
    out = _run(cmd)
    if not out:
        return {}
    best = {}
    for ln in out.splitlines():
        parts = ln.split('|')
        parts += ['']*8
        U,A,DefQOS,MaxTRESPU,GrpTRES,GrpJobs,GrpTRESMins,GrpWall = parts[:8]
        if U != user:
            continue
        if account and A == account:
            return {
                'User':U,'Account':A,'DefQOS':DefQOS,'MaxTRESPU':MaxTRESPU,
                'GrpTRES':GrpTRES,'GrpJobs':GrpJobs,'GrpTRESMins':GrpTRESMins,'GrpWall':GrpWall
            }
        # fallback: keep the first row for user
        if not best:
            best = {
                'User':U,'Account':A,'DefQOS':DefQOS,'MaxTRESPU':MaxTRESPU,
                'GrpTRES':GrpTRES,'GrpJobs':GrpJobs,'GrpTRESMins':GrpTRESMins,'GrpWall':GrpWall
            }
    return best

def explain_pending(jobid: str):
    """
    Print a compact, color-coded explanation for why a job is pending (or not),
    with priority/fairshare and key limits that may apply.
    """
    sq   = _squeue_details(jobid)
    if not sq.get('JobID'):
        _console.print(Panel(f"[red]Job {jobid} not found[/red]"))
        return

    sc   = _scontrol_job(jobid)
    sp   = _sprio(jobid)
    user = sq.get('User') or sc.get('UserId','').split('(')[0]
    qos  = sq.get('QoS') or sc.get('QOS')
    acct = sc.get('Account') or sc.get('AccrueAssocId')

    sshare_row = _sshare_for(user) or {}
    qos_row    = _qos_limits(qos or '')
    assoc_row  = _assoc_limits(user or '', acct)

    # Section 1: headline
    head = Table.grid(expand=True)
    head.add_column(justify="left")
    head.add_row(f"[bold]Job[/bold] {sq.get('JobID','?')}  "
                 f"[bold]{sq.get('State','?')}[/bold]  "
                 f"[dim]Reason:[/dim] {sq.get('Reason','?')}")
    
    # Section 2: job facts
    facts = Table.grid()
    facts.add_column(style="bold cyan")
    facts.add_column()
    facts.add_row("Partition", sq.get('Partition','?'))
    facts.add_row("QoS",       qos or '?')
    facts.add_row("User",      user or '?')
    facts.add_row("GRES",      sq.get('GRES','?'))
    facts.add_row("Nodes/CPUs",f"{sq.get('Nodes','?')}/{sq.get('CPUs','?')}")
    facts.add_row("TimeLimit", sq.get('TimeLimit','?'))
    facts.add_row("Submitted", sq.get('Submitted','?'))

    # Section 3: priority
    prio = Table.grid()
    prio.add_column(style="bold magenta")
    prio.add_column()
    if sp:
        for k,v in sp.items():
            prio.add_row(k, v)
    else:
        prio.add_row("INFO","No sprio data")

    # Section 4: limits snapshot
    lim = Table.grid()
    lim.add_column(style="bold yellow")
    lim.add_column()
    if qos_row:
        lim.add_row("QOS", (qos_row.get('Name','') or qos) )
        for k in ['Priority','MaxTRESPU','MaxTRES','GrpTRES','MaxJobs','MaxSubmitJobs','Preempt']:
            if qos_row.get(k):
                lim.add_row(f"QOS.{k}", qos_row[k])
    if assoc_row:
        for k in ['User','Account','DefQOS','MaxTRESPU','GrpTRES','GrpJobs','GrpTRESMins','GrpWall']:
            if assoc_row.get(k):
                lim.add_row(f"Assoc.{k}", assoc_row[k])

    # Section 5: fairshare snapshot
    fs = Table.grid()
    fs.add_column(style="bold green")
    fs.add_column()
    if sshare_row:
        for k,v in sshare_row.items():
            fs.add_row(k, v)
    else:
        fs.add_row("sshare","(no data)")

    # Combine
    _console.print(Panel(head, title="Pending Explanation"))
    _console.print(Columns([facts, prio, lim, fs], expand=True, equal=True))

def main():
    """Main function for standalone execution."""
    args = parser.parse_args()
    
    if args.explain:
        explain_pending(args.explain)
    elif args.explain_latest:
        user = _current_user()
        jobid = _find_latest_pending_jobid_for_user(user)
        if jobid:
            explain_pending(jobid)
        else:
            _console.print("[yellow]No pending job found to explain.[/yellow]")
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
