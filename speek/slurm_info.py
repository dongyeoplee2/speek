import subprocess
from typing import Optional

from parsers import parse_datetime_str, parse_delta_str, parse_info_tree

__INFO_FIELD_INTERESTING = [
    'JobID', 'JobName', 'User', 'UID', 'Partition',
    'State', 'ExitCode', 'Reason',
    # 'AllocCPUs', 'AllocNodes', 'TotalCPU', 'NodeList',
    # 'ReqMem', 'ReqGRES', , 'NTasksPerNode', 'TRESBillingWeights', 
    'SubmitTime', 'StartTime', 'EndTime', 'Elapsed', 
    'Command', 'WorkDir', 'StdOut'
]

def get_user_ids():
    user_str = subprocess.check_output(['sacctmgr', 'show', 'user', '-P']).decode('utf-8').replace(' ','\n')
    header, *infos = [s.split('|') for s in user_str.split('\n')]
    users = [i[0] for i in infos]
    return users


def filter_fields(sacct_dicts, fields=__INFO_FIELD_INTERESTING):
    """Filter the sacct_dict to only include fields of interest."""
    for k, sacct_dict in sacct_dicts.items():
        sacct_dicts[k] = {k: v for k, v in sacct_dict.items() if k in fields}
    return sacct_dicts




def get_sacct_format_option_str():
    sacct_str = subprocess.check_output(['sacct', '--helpformat']).decode('utf-8')
    sacct_options = sacct_str.replace('\n', ' ').split()
    return '--format=' + ','.join(sacct_options)

def _sacct_val_str_resolve_conflict(v_core, v_new, keep=False):
    if keep:
        return (*v_core, v_new)
    if v_core==v_new:
        return v_core
    
    TRIVIAL_KEYWORDS_RANK = [
        ['Unknown', 'None', 'None assigned'],    # rank 0
        ['Cyclic', 'batch', 'hostname', 'pwd', 'date', 'normal'], # rank 1
    ]
    def ranker(v):
        if not v:
            return 0
        for i, l in enumerate(TRIVIAL_KEYWORDS_RANK):
            if v in l:
                return i
        else:
            return i+1
    
    v_core_rank = ranker(v_core)
    v_new_rank = ranker(v_new)
    
    if v_core_rank != v_new_rank:
        return max([v_core, v_new], key=ranker)
    
    return v_core
    
    # return max([v_core, v_new], key=len)

def sacct_merge_jobid_batch(sacct_dicts, keep=False):
    
    merged_dicts = {}
    for jobid, sacct_dict in sacct_dicts.items():
        if '.' not in jobid:
            jobid += '.'
        jobid_core, aux = jobid.split('.')
        jobid_core = int(jobid_core)
        
        if jobid_core not in merged_dicts:
            if keep:
                sacct_dict = {k: (v,) for k, v in sacct_dict.items()}
            sacct_dict['auxs'] = [aux]
            merged_dicts[jobid_core] = sacct_dict
            continue
    
        d = merged_dicts.pop(jobid_core)
        d_ = sacct_dict
        d_ = {
            k: _sacct_val_str_resolve_conflict(d[k], d_[k], keep=keep) for k in sacct_dict.keys()
        }
        d_['auxs'] = d['auxs'] + [aux]
        merged_dicts[jobid_core] = d_
    
    return merged_dicts
    


def get_sacct_dict(
    time_span: str='3-00:00:00',
    start: Optional[str]=None,
    end: Optional[str]=None,
    user: Optional[str]=None,
    keep_jobid_batch: bool=False
):
    """sacct -S 2025-05-31 -E now -a --user dylee23  --long --parsable
    sacct -S 2025-05-31 -E 2025-06-0312:10:00 -a --user dylee23  --long --parsable
    """
    end = end or 'now'
    e_dt = parse_datetime_str(end, '%Y-%m-%d%H:%M:%S')
    
    if isinstance(start, str):
        parse_datetime_str(time_span, '%d-%H:%M:%S')
    else:
        time_span = parse_delta_str(time_span)
        start = (e_dt - time_span).strftime('%Y-%m-%d%H:%M:%S')
    
    form_str = get_sacct_format_option_str()
    sacct_command = ['sacct', '-S', start, '-E', end, form_str, '--parsable2']
    if user:
        sacct_command += ['--user', user]
        
    sacct_str = subprocess.check_output(sacct_command).decode('utf-8')
    header, *rows = [r.split('|') for r in sacct_str.split('\n') if r]
    sacct_dicts = [{k: v for k, v in zip(header[1:], row[1:])} for row in rows]
    sacct_dicts = {d['JobID']: d for d in sacct_dicts}
    
    sacct_dicts = sacct_merge_jobid_batch(sacct_dicts, keep=keep_jobid_batch)
    
    return sacct_dicts


def process_stdout(info_dict):
  for jid, info in info_dict.items():
    repl = lambda s: s.replace('%j', str(jid))
    if isinstance(info['StdOut'], str):
      info['StdOut'] = repl(info['StdOut'])
    elif isinstance(info['StdOut'], tuple):
      info['StdOut'] = tuple(map(repl, info['StdOut']))
  return info_dict
      


def get_scontrol_dict(unit):
    assert unit in ['Job', 'Partition', 'Node']
    
    scontrol_str = subprocess.check_output(['scontrol', 'show', unit]).decode('utf-8').replace(' ', '\n')
    
    scontrol_dicts =  {}
    delimiter = f'{unit}Name=' if unit != 'Job' else 'JobId='
    for scontrol in scontrol_str.split(delimiter):
        if not scontrol: continue
        n, *infos = [i for i in scontrol.split('\n') if i]
        if unit == 'Job': n = int(n) if n!='No' else 0
        
        scontrol_dicts[n] = {}
        for info in infos:
            if '=' not in info:
                scontrol_dicts[n][info] = None
                continue
            k, v = info.split('=', 1)
            if ',' not in v or '[' in v:
                scontrol_dicts[n][k] = v
            elif '=' in v:
                scontrol_dicts[n][k] = dict([i.split('=') for i in v.split(',')])
            else:
                scontrol_dicts[n][k] = tuple(v.split(','))
    return scontrol_dicts


def get_slurm_infos(
    time_span: str='3-00:00:00',
    start: Optional[str]=None,
    end: Optional[str]=None,
    user: Optional[str]=None,
    parse: bool=True,
    keep_jobid_batch: bool=False
):
    """Get slurm infos from sacct and scontrol."""
    sacct_dicts = get_sacct_dict(time_span, start, end, user, keep_jobid_batch)
    scontrol_dicts = get_scontrol_dict('Job')
    # scontrol_dicts = {}
    
    infos = {
        k: {
            **sacct_dict,
            **scontrol_dicts.get(k, {})
        } for k, sacct_dict in sacct_dicts.items()
    }
    infos = process_stdout(infos)
    if parse:
        infos = parse_info_tree(infos)
    
    return infos
