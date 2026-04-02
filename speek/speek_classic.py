import subprocess
from glob import glob
import csv
import re

import argparse
from datetime import datetime, timedelta

from rich import print
from rich.table import Table
from rich.align import Align
from rich.live import Live
from rich.console import Group
from rich.panel import Panel
from rich.text import Text as RText

parser = argparse.ArgumentParser(description="Peek into slurm resource info.")
parser.add_argument('-u', '--user', default=None, type=str, help='Specify highlighted user.')

parser.add_argument('-l', '--live', action='store_true', help='Live display of speek every 1 seconds.')
parser.set_defaults(live=False)

parser.add_argument('-f', '--file', default='auto', type=str, help='Specify file for user info.')
parser.add_argument('-t', '--t_avail', default='5 m', type=str, help='Time window width for upcomming release in {m:minutes, h:hours, d:days}. (default: 5 m)')
args = parser.parse_args()


def get_scontrol_dict(unit):
    assert unit in ['Job', 'Partition', 'Node']

    scontrol_str = subprocess.check_output(
        ['scontrol', 'show', unit, '--oneliner'],
        text=True, stderr=subprocess.DEVNULL,
    )

    scontrols = {}
    for line in scontrol_str.splitlines():
        line = line.strip()
        if not line:
            continue
        # Each line is one record; split on spaces to get key=value tokens
        tokens = line.split()
        record = {}
        for token in tokens:
            if '=' not in token:
                continue
            k, v = token.split('=', 1)
            if ',' not in v or '[' in v:
                record[k] = v
            elif '=' in v:
                try:
                    record[k] = dict([i.split('=', 1) for i in v.split(',')])
                except ValueError:
                    record[k] = v
            else:
                record[k] = tuple(v.split(','))

        # Extract the record key
        if unit == 'Job':
            n = record.pop('JobId', None)
            if n is None:
                continue
            try:
                n = int(n)
            except ValueError:
                continue
        else:
            n = record.pop(f'{unit}Name', None)
            if n is None:
                continue
        scontrols[n] = record
    return scontrols

def td_parse(s):
   dt = datetime.strptime(s, '%d-%H:%M:%S') if '-' in s else datetime.strptime(s, '%H:%M:%S') 
   return timedelta(days=dt.day, hours=dt.hour, minutes=dt.minute, seconds=dt.second)


def consecutor(lst):
    assert all([isinstance(i, (int, float)) for i in lst]), 'List should be all numbers.'
    lst.sort()
    if len(lst)==0: return ''
    pi, *ll = lst
    cl = [[pi]]
    for i in ll:
        if i-pi>1: cl.append([i])
        else: cl[-1].append(i)
        pi = i
    l_str = ' '.join([f'{{{c[0]}..{c[-1]}}}' if len(c)>1 else f'{c[0]}' for c in cl])
    return l_str


def _sniff_gres_key(partitions):
    """Scan all partitions to find the correct case of the gres/gpu key in TRESBillingWeights.
    Handles both dict form (multiple weights) and string form (single weight).
    Returns 'gres/gpu' as a safe default if no GPU billing weight is found."""
    for info in partitions.values():
        tbw = info.get('TRESBillingWeights')
        if not tbw:
            continue
        for key in ['GRES/gpu', 'gres/gpu']:
            if isinstance(tbw, dict) and key in tbw:
                return key
            if isinstance(tbw, str) and key in tbw:
                return key
    return 'gres/gpu'


def _gpu_weight(partitions, partition_name, gres_key):
    """Safely get GPU billing weight for a partition as a float.
    Returns 0.0 if TRESBillingWeights is absent, None, or the key is missing."""
    tbw = partitions.get(partition_name, {}).get('TRESBillingWeights')
    if isinstance(tbw, dict):
        try:
            return float(tbw.get(gres_key, 0))
        except (ValueError, TypeError):
            return 0.0
    if isinstance(tbw, str):
        m = re.search(rf'{re.escape(gres_key)}=([0-9.]+)', tbw)
        return float(m.group(1)) if m else 0.0
    return 0.0


def _parse_userid(userid_str):
    """Extract bare username from scontrol UserId field.
    Handles formats: 'user(uid)', 'DOMAIN\\user(uid)'."""
    name = (userid_str or '').split('(')[0].strip()
    return name.split('\\')[-1]


def _parse_gpu_count(tres_per_node):
    """Extract GPU count from TresPerNode field.
    Handles 'gres:gpu:N', 'gres/gpu:N', 'gres:gpu:MODEL:N', and multi-GRES strings.
    Both colon and slash separators between 'gres' and 'gpu' are supported."""
    if isinstance(tres_per_node, (list, tuple)):
        tres_per_node = ','.join(str(v) for v in tres_per_node)
    m = re.search(r'gres[:/]gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', tres_per_node or '')
    return int(m.group(1)) if m else 0


def _node_gpu_usage():
    """Query scontrol show node for actual per-partition GPU total/used.
    Returns {partition: (total, used)}."""
    try:
        out = subprocess.check_output(
            ['scontrol', 'show', 'node', '--oneliner'],
            text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return {}
    result: Dict[str, Tuple[int, int]] = {}
    for ln in out.splitlines():
        if not ln.strip():
            continue
        def _f(key):
            m = re.search(rf'{key}=(\S+)', ln)
            return m.group(1) if m else ''
        parts_str = _f('Partitions')
        cfg = _f('CfgTRES')
        alloc = _f('AllocTRES')
        gres_used = _f('GresUsed')
        # Total
        mg = re.search(r'gres/gpu=(\d+)', cfg)
        total = int(mg.group(1)) if mg else 0
        if total == 0:
            continue
        # Used
        mu = re.search(r'gres/gpu=(\d+)', alloc)
        if mu:
            used = int(mu.group(1))
        else:
            gu = re.search(r'gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', gres_used or '', re.IGNORECASE)
            used = int(gu.group(1)) if gu else 0
        used = min(used, total)
        for p in parts_str.split(','):
            p = p.strip()
            if not p:
                continue
            prev = result.get(p, (0, 0))
            result[p] = (prev[0] + total, prev[1] + used)
    return result


def compute_gpu_stats(partitions, jobs, tw):
    """
    Core computation: given parsed scontrol dicts and a release time-window,
    return (gpu_resource, user_status, user_job_status, gres).

    gpu_resource keys:
      'Available', 'Total', 'Usage'  – cluster-wide scalars
      '<partition>'                  – per-partition dict with same keys + 'Upcomming release'
    user_status keys:
      '<user>' -> {'RUNNING': N, 'PENDING': N, '<partition>': {'RUNNING': N, 'PENDING': N}}
    """
    gres = _sniff_gres_key(partitions)

    status = {'PENDING', 'RUNNING'}
    resource = {'Available', 'Total', 'Usage', 'max_user'}

    NewState = lambda fields: {k: 0 for k in fields}

    user_status, gpu_resource = {}, NewState(resource)
    user_job_status = {}

    # Get actual node-level GPU usage (authoritative source)
    node_usage = _node_gpu_usage()

    if jobs:
        for id, job in jobs.items():
            j_status = job.get('JobState', None)
            
            if j_status in status:
                job_name = job['JobName']
                user, gpu = _parse_userid(job['UserId']), job['Partition']
                gpu_count = _parse_gpu_count(job.get('TresPerNode'))
                
                if isinstance(gpu, tuple):
                    gpu = tuple(sorted(gpu, key=lambda x: _gpu_weight(partitions, x, gres), reverse=True))
                    gpu_one = gpu[0]
                else:
                    gpu_one = gpu

                if _gpu_weight(partitions, gpu_one, gres) == 0.0: continue
                
                # user status
                u_stat = user_status.get(user, NewState(status))
                u_stat[gpu_one] = u_stat.get(gpu_one, NewState(status))
                
                u_stat[j_status] += gpu_count
                u_stat[gpu_one][j_status] += gpu_count
                
                user_status[user] = u_stat
                
                uj_stat = user_job_status.get(user, {})
                uj_stat[job_name] = uj_stat.get(job_name, {})
                
                uj_stat[job_name][gpu] = uj_stat[job_name].get(gpu, {s:[] for s in status})
                uj_stat[job_name][gpu][j_status].append((id, gpu_count))
                
                user_job_status[user] = uj_stat

                # gpu status
                gpu_resource[gpu] = gpu_resource.get(gpu, NewState(resource))

                if j_status=='RUNNING':
                    time_left = {'td': td_parse(job['TimeLimit'])- td_parse(job['RunTime']),
                                'count': gpu_count, 'user': user}

                    up_re = gpu_resource[gpu].get('Upcomming release', [time_left, [time_left]])
                    up_re[0] = min(time_left, up_re[0], key=lambda x: x['td'])

                    up_re[1].append(time_left)
                    up_re[1] = [t for t in up_re[1] if t['td']-up_re[0]['td']<tw]

                    up_re[0]['total_count'] = sum([t['count'] for t in up_re[1]])
                    td = up_re[0]['td']
                    up_re[0]['str'] = (f'{td.days}-' if td.days else '') + f"{str(td).split(', ')[-1][:-3]} ({up_re[0]['total_count']})"

                    gpu_resource[gpu]['Upcomming release'] = up_re


    for gpu, info in partitions.items():
        if _gpu_weight(partitions, gpu, gres) == 0.0: continue
        tres = info.get('TRES')
        count = int(tres.get('gres/gpu', 0)) if isinstance(tres, dict) else 0
        if count == 0: continue

        gpu_resource[gpu] = gpu_resource.get(gpu, NewState(resource))

        # Total from partition TRES (authoritative for partition-level total)
        # Used from node state (authoritative for actual allocation)
        nu = node_usage.get(gpu, None)
        if nu:
            _nu_total, used = nu
            avail = max(count - used, 0)
        else:
            avail = count  # fallback: assume all free

        gpu_resource['Total'] += count
        gpu_resource[gpu]['Total'] += count
        gpu_resource['Available'] += avail
        gpu_resource[gpu]['Available'] += avail

        gpu_resource['Usage'] = f"{(gpu_resource['Total'] - gpu_resource['Available'])/gpu_resource['Total']*100:.2f}%"
        gpu_resource[gpu]['Usage'] = f"{(gpu_resource[gpu]['Total'] - gpu_resource[gpu]['Available'])/gpu_resource[gpu]['Total']*100:.2f}%"
        
        for s in status:
            max_user = max(user_status.items(), key=lambda x: x[1].get(gpu, NewState(status))[s]) if user_status else (None, NewState(status))
            gpu_resource[gpu][f'max_{s}_user'] = max_user[0] if max_user[1].get(gpu, NewState(status))[s] else None

    return gpu_resource, user_status, user_job_status, gres


def get_slurm_resource():
    ##############################################
    #               get user info                #
    ##############################################

    # who am I
    me = args.user
    if me==None:
        me = subprocess.check_output(['whoami']).decode('utf-8').strip()

    # who are they
    paths = glob(args.file)

    if paths:
        with open(paths[0], 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header, *users = list(reader)

        user_info = [dict(zip(header, user)) for user in users]
        user_lookup = {}
        for user in user_info:
            if not user['name']: continue
            user_lookup[user['user']] = f"{user['name']} ({user['affiliation'].split('-')[0][:2]} {user['title']}, {user['user']})"
    else:
        user_lookup = {}

    partitions, jobs = map(get_scontrol_dict, ('Partition', 'Job'))

    td_str = {'m':'minutes', 'h':'hours', 'd':'days'}
    t_width, t_unit = args.t_avail.split()
    tw = timedelta(**{td_str[t_unit]: int(t_width)})

    gpu_resource, user_status, user_job_status, gres = compute_gpu_stats(partitions, jobs, tw)

    status = {'PENDING', 'RUNNING'}
    resource = {'Available', 'Total', 'Usage', 'max_user'}
    NewState = lambda fields: {k: 0 for k in fields}

    ####################################################
    #                print usage table                 #
    ####################################################

    tables = []

    ranking = {0:'🥇', 1:'🥈', 2:'🥉'}
    pareto = '🚩'
    king = {'RUNNING':'', 'PENDING':''}

    def _util_color(pct):
        """Utilization color: green < 50%, yellow 50-99%, red 100%."""
        if pct >= 100: return 'red'
        if pct >= 50:  return 'yellow'
        return 'green'

    from rich.box import MINIMAL
    table1 = Table(show_header=True, show_edge=False, box=MINIMAL,
                   pad_edge=False, padding=(0, 1, 0, 0), border_style='#777777')

    # add columns
    partitions_list = [p for p in sorted({*partitions.keys()} - resource) if _gpu_weight(partitions, p, gres) > 0.0]
    partitions_list = sorted(partitions_list, key=lambda x: gpu_resource[x]['Total'] * _gpu_weight(partitions, x, gres), reverse=True)
    table1.add_column("User", style='bold', no_wrap=True)
    def _state_emoji(pct):
        if pct == 100: return '☠️ '
        if pct > 90:   return '🔥'
        if pct == 0:   return '🏖️ '
        if pct < 10:   return '❄️ '
        return ''

    for i, p in enumerate(partitions_list):
        pct = float(gpu_resource[p]['Usage'][:-1])
        uc = _util_color(pct)
        table1.add_column(RText(_state_emoji(pct) + p, style=f'bold {uc}'), justify="right", no_wrap=True)
    table1.add_column("Total", justify="right", style='bold', no_wrap=True)

    # add rows — GPU counts colored by utilization
    def _gpu_cell(avail, total):
        t = RText()
        pct = (total - avail) / total * 100 if total else 0
        uc = _util_color(pct)
        t.append(str(avail), style=f'bold {uc}')
        t.append(f'/{total}', style='bright_black')
        return t

    def _usage_cell(usage_str):
        pct = float(usage_str[:-1])
        uc = _util_color(pct)
        return RText(usage_str, style=f'bold {uc}')

    table1.add_row(RText('GPUs', style='dim', justify='right'),
                   *[_gpu_cell(gpu_resource[p]['Available'], gpu_resource[p]['Total']) for p in partitions_list],
                   _gpu_cell(gpu_resource['Available'], gpu_resource['Total']))
    table1.add_row(RText('Usage', style='dim', justify='right'),
                   *[_usage_cell(gpu_resource[p]['Usage']) for p in partitions_list],
                   _usage_cell(gpu_resource['Usage']), end_section=True)

    user_status_sorted = sorted(user_status.items(), key=lambda x: (x[1]['RUNNING'], x[1]['PENDING']), reverse=True)
    agg_running = 0
    for i, (user, info) in enumerate(user_status_sorted):
        all_running = v if agg_running<(v:=gpu_resource['Total']-gpu_resource['Available'])*0.8 else float('inf')
        agg_running += info['RUNNING']
        
        style="on bright_black" if i%2 else ""
        if user==me:
            style="black on bright_green"
        
        me_section = (me in {user, user_status_sorted[min(i+1, len(user_status_sorted)-1)][0]})
            
        rank = ranking.get(i, i+1 if agg_running<all_running*0.8 else pareto)
        
        user_true = user_lookup.get(user, user)
        def _state_text(state):
            t = RText()
            r = state['RUNNING']
            p = state['PENDING']
            if r: t.append(f'▶{r}', style='bold green')
            if r and p: t.append(' ')
            if p: t.append(f'⏸{p}', style='bold yellow')
            return t
        king_str = lambda p: ''.join([king[s] for s in sorted(status) if user==gpu_resource[p][f'max_{s}_user']])
        cells = []
        for p in partitions_list:
            st = _state_text(info.get(p, NewState(status)))
            ks = king_str(p)
            if ks:
                t = RText(ks)
                t.append_text(st)
                cells.append(t)
            else:
                cells.append(st)
        table1.add_row(f'{rank:>2}. {user_true}', *cells, _state_text(info), style=style, end_section=me_section)
    
    tables.append(Align.center(Panel(
        table1,
        title='[bold]Cluster Usage[/bold]',
        border_style='bright_blue',
        padding=(0, 1),
        expand=False,
    )))
    # print(' \n ')
    # print(Align(table1, align='center'))


    ##################################################
    #                print job table                 #
    ##################################################

    jobs = user_job_status.get(me, {})

    if jobs:
        table2 = Table(show_header=True, show_edge=False, box=MINIMAL,
                       pad_edge=False, padding=(0, 1, 0, 0), border_style='#777777')

        table2.add_column('St', style='bold', no_wrap=True)
        table2.add_column('Job', no_wrap=True)
        table2.add_column('GPU', no_wrap=True)
        table2.add_column('#', justify='right', no_wrap=True)
        table2.add_column('IDs', no_wrap=True, max_width=30)
        for s in sorted(status, reverse=True):
            jobs_f = {k: {jn: j for jn, j in v.items() if j[s]} for k, v in jobs.items() if any(j[s] for j in v.values())}
            for i, (job_name, job) in enumerate(jobs_f.items()):
                def keykey(gpu):
                    if isinstance(gpu, tuple):
                        gpu = sorted(gpu, key=lambda x: _gpu_weight(partitions, x, gres))[-1]
                    return gpu_resource[gpu]['Total'] * _gpu_weight(partitions, gpu, gres)
                job_sorted = sorted(job.keys(), key=keykey, reverse=True)
                # job_sorted = sorted(job.keys(), key=lambda x: gpu_resource[x]['Total']*float(partitions[x]['TRESBillingWeights'][gres]), reverse=True)
                for j, gpu in enumerate(job_sorted):
                    ids = job[gpu][s]
                    if isinstance(gpu, tuple):
                        gpu = '{' + ',\n '.join(gpu) + '}'
                    _sym = '▶' if s == 'RUNNING' else '⏸' if s == 'PENDING' else s
                    _scol = 'bold green' if s == 'RUNNING' else 'bold yellow' if s == 'PENDING' else ''
                    table2.add_row(RText(_sym, style=_scol) if i+j==0 else RText(''), job_name if j==0 else '', gpu, str(len(ids)), consecutor([id for id, _ in ids]), end_section=((i==len(jobs_f)-1) and (j==len(job_sorted)-1)))
            
        # print(' \n ')
        # print(Align(table2, align='center'))
        # print(' \n ')
        
        tables.append(Align.center(Panel(
            table2,
            title=f'[bold]{user_lookup.get(me, me)}\'s Jobs[/bold]',
            border_style='bright_blue',
            padding=(0, 1),
            expand=False,
        )))

    # table3 = Table(title="Job Status")

    # table3.add_column("User")
    # table3.add_column("#")
    # table3.add_column("GPUs")
    # table3.add_column("Status")
    # table3.add_column("Status")

    # user_job_status_sorted = [(user, user_job_status[user]) for user, _ in user_status_sorted]
    # for i, (user, jobs) in enumerate(user_job_status_sorted):
    #     style="on bright_black" if i%2 else ""
    #     if user==me:
    #         style="black on bright_green"
        
    #     me_section = (me in {user, user_status_sorted[min(i+1, len(user_status_sorted)-1)][0]})
        
    #     j_gpu, j_status = [], []
    #     for job_name, job in jobs.items():
    #         j_str = lambda s: 'P' if s=='PENDING' else 'R' if s=='RUNNING' else ''
    #         j_gpu.append('['+', '.join([f'{k} ({" ".join([j_str(j)+str(sum([cc[1] for cc in c])) for j, c in sorted(v.items(), reverse=True)])})' for k, v in job.items()])+']')
    #         j_status.append(' [R ' + ', '.join([consecutor([id for id, _ in ids]) for _, v in job.items() for s, ids in v.items() if s=='RUNNING']) + ']' +
    #                         ' [P ' + ' '.join([consecutor([id for id, _ in ids]) for _, v in job.items() for s, ids in v.items() if s=='PENDING']) + ']')
    #     if user==me:
    #         table3.add_row(user_lookup.get(user, user), str(len(jobs.items())), '\n'.join(jobs.keys()), '\n'.join(j_gpu), '\n'.join(j_status), style=style, end_section=me_section)
    #     else:
    #         table3.add_row(user_lookup.get(user, user), str(len(jobs.items())), '\n'.join(jobs.keys()), '\n'.join(j_gpu), '\n'.join(j_status), style=style, end_section=me_section)
    #         # table3.add_row(user_lookup.get(user, user), str(len(jobs.items())), ' / '.join(jobs.keys()), style=style, end_section=me_section)

    # print(Align(table3, align='center'))
    
    return Group(*tables)

def main():
    if args.live:
        with Live(get_slurm_resource(), refresh_per_second=1) as live:
            while True:
                live.update(get_slurm_resource())
    else:
        print(get_slurm_resource())
    
if __name__ == '__main__':
    main()