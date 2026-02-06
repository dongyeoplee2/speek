import subprocess
from glob import glob
import csv
import re

from typing import Optional

import argparse
from datetime import datetime, timedelta

from rich import print
from rich.table import Table
from rich.align import Align
from rich.live import Live
from rich.console import Group

from parsers import parse_delta_str
from slurm_info import get_scontrol_dict

parser = argparse.ArgumentParser(description="Peek into slurm resource info.")
parser.add_argument('-u', '--user', default=None, type=str, help='Specify highlighted user.')

parser.add_argument('-l', '--live', action='store_true', help='Live display of speek every 1 seconds.')
parser.set_defaults(live=False)

parser.add_argument('-f', '--file', default='auto', type=str, help='Specify file for user info.')
parser.add_argument('-t', '--t_avail', default='5 m', type=str, help='Time window width for upcomming release in {m:minutes, h:hours, d:days}. (default: 5 m)')
args = parser.parse_args()



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


    ##############################################
    #               get gpu status               #
    ##############################################

    partitions, jobs = map(get_scontrol_dict, ('Partition', 'Job'))
    
    from pandas import DataFrame
    print(DataFrame.from_dict(jobs, orient='index'))
        
    gres_names = ['GRES/gpu', 'gres/gpu']
    
    for gres in gres_names:
        if gres in partitions[[*partitions.keys()][0]]['TRESBillingWeights']:
            break 

    # partitions = {k: v for k, v in partitions.items() if 'cpu' not in k}

    status = {'PENDING', 'RUNNING'}
    resource = {'Available', 'Total', 'Usage', 'max_user'}
    release = {'Time left', 'count', 'user'}

    NewState = lambda fields: {k: 0 for k in fields}

    user_status, gpu_resource = {}, NewState(resource)
    user_job_status = {}

    current_time = datetime.now()

    td_str = {'m':'minutes', 'h':'hours', 'd':'days'}
    t_width, t_unit = args.t_avail.split()
    tw = timedelta(**{td_str[t_unit]: int(t_width)})

    if jobs:
        for id, job in jobs.items():
            j_status = job.get('JobState', None)
            
            if j_status in status:
                job_name = job['JobName']
                user, gpu = job['UserId'].split('(')[0].strip(), job['Partition']
                gpu_count = int(re.split(':|=', job.get('TresPerNode', 'gres:gpu:0'))[-1])
                
                if isinstance(gpu, tuple):
                    gpu = tuple(sorted(gpu, key=lambda x: float(partitions[x]['TRESBillingWeights'][gres]), reverse=True))
                    gpu_one = gpu[0]
                else:
                    gpu_one = gpu
                
                if partitions[gpu_one]['TRESBillingWeights'][gres]=='0': continue
                
                # user status
                u_stat = user_status.get(user, NewState(status))
                u_stat[gpu_one] = u_stat.get(gpu, NewState(status))
                
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
                    gpu_resource['Available'] -= gpu_count
                    gpu_resource[gpu]['Available'] -= gpu_count
                
                    time_left = {'td': parse_delta_str(job['TimeLimit'])- parse_delta_str(job['RunTime']),
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
        if info['TRESBillingWeights'][gres]=='0': continue
        count = int(info['TRES']['gres/gpu'])
        
        gpu_resource[gpu] = gpu_resource.get(gpu, NewState(resource))
        
        for s in ['Available', 'Total']:
            gpu_resource[s] += count
            gpu_resource[gpu][s] += count
        
        gpu_resource['Usage'] = f"{(gpu_resource['Total'] - gpu_resource['Available'])/gpu_resource['Total']*100:.2f}%"
        gpu_resource[gpu]['Usage'] = f"{(gpu_resource[gpu]['Total'] - gpu_resource[gpu]['Available'])/gpu_resource[gpu]['Total']*100:.2f}%"
        
        for s in status:
            max_user = max(user_status.items(), key=lambda x: x[1].get(gpu, NewState(status))[s]) if user_status else (None, NewState(status))
            gpu_resource[gpu][f'max_{s}_user'] = max_user[0] if max_user[1].get(gpu, NewState(status))[s] else None

    ####################################################
    #                print usage table                 #
    ####################################################
    
    tables = []

    ranking = {0:'🥇', 1:'🥈', 2:'🥉'}
    get_state = lambda p: ('☠️ ' if p==100 else '🔥' if p>90 else '🏖️ ' if p==0 else '❄️ ' if p<10 else '') #+' '
    pareto = '🚩'
    king = {'RUNNING':'👑', 'PENDING':'⏳'}

    table1 = Table(title="Cluster Usage")

    # add columns
    partitions_list = [p for p in sorted({*partitions.keys()} - resource) if partitions[p]['TRESBillingWeights'][gres]!='0']
    partitions_list = sorted(partitions_list, key=lambda x: gpu_resource[x]['Total']*float(partitions[x]['TRESBillingWeights'][gres]), reverse=True)
    table1.add_column("User")
    for i, p in enumerate(partitions_list):
        table1.add_column(get_state(float(gpu_resource[p]['Usage'][:-1])) + p, justify="right")
    table1.add_column("Total", justify="right")


    # add rows
    for f in ['Available', 'Total', 'Usage']:
        table1.add_row(f, *[str(gpu_resource[p][f]) for p in partitions_list], str(gpu_resource[f]))
    table1.add_row(f'Until release (~{t_width}{td_str[t_unit][0]})', *[gpu_resource[p].get('Upcomming release', [{}])[0].get('str', '') for p in partitions_list], '', end_section=True)

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
        state_str = lambda state: (f"{v}" if (v:=state['RUNNING']) else '') + (f"({v})" if (v:=state['PENDING']) else '')
        king_str = lambda p: ''.join([king[s] for s in sorted(status) if user==gpu_resource[p][f'max_{s}_user']])
        table1.add_row(f'{rank:>2}. {user_true}', *[king_str(p)+state_str(info.get(p, NewState(status))) for p in partitions_list], state_str(info), style=style, end_section=me_section)
    
    tables.append(' \n')
    tables.append(Align(table1, align='center'))
    # print(' \n ')
    # print(Align(table1, align='center'))


    ##################################################
    #                print job table                 #
    ##################################################

    jobs = user_job_status.get(me, {})

    if jobs:
        table2 = Table(title=f"{user_lookup.get(me, me)}'s Job Status")

        for c in ['Status', 'Job', 'GPU', '#', 'ids']:
            table2.add_column(c)
        for s in sorted(status, reverse=True):
            jobs_f = {k: {jn: j for jn, j in v.items() if j[s]} for k, v in jobs.items() if any(j[s] for j in v.values())}
            for i, (job_name, job) in enumerate(jobs_f.items()):
                def keykey(gpu):
                    if isinstance(gpu, tuple):
                        gpu = sorted(gpu, key=lambda x: float(partitions[x]['TRESBillingWeights'][gres]))[-1]
                    return gpu_resource[gpu]['Total']*float(partitions[gpu]['TRESBillingWeights'][gres])
                job_sorted = sorted(job.keys(), key=keykey, reverse=True)
                # job_sorted = sorted(job.keys(), key=lambda x: gpu_resource[x]['Total']*float(partitions[x]['TRESBillingWeights'][gres]), reverse=True)
                for j, gpu in enumerate(job_sorted):
                    ids = job[gpu][s]
                    if isinstance(gpu, tuple):
                        gpu = '{' + ',\n '.join(gpu) + '}'
                    table2.add_row(s if i+j==0 else '', job_name if j==0 else '', gpu, str(len(ids)), consecutor([id for id, _ in ids]), end_section=((i==len(jobs_f)-1) and (j==len(job_sorted)-1)))
            
        # print(' \n ')
        # print(Align(table2, align='center'))
        # print(' \n ')
        
        tables.append(' \n ')
        tables.append(Align(table2, align='center'))
        tables.append(' \n ')

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