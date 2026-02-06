from typing import Optional
from datetime import datetime, timedelta


def parse_datetime_str(date_str: Optional[str], fmt: str = '%Y-%m-%dT%H:%M:%S'):
    """Assert that the date_str is in the specified format."""
    try:
        if date_str == None or date_str == 'now':
            return datetime.now()
        return datetime.strptime(date_str, fmt)
    except ValueError:
        raise ValueError(f"Date string '{date_str}' does not match format '{fmt}'.")


def parse_delta_str(delta_str: str):
    """Assert that the delta_str is in the specified format."""
    delta_dict = {}
    if '-' in delta_str:
        day, delta_str = delta_str.split('-')
        delta_dict['days'] = int(day)
    delta = delta_str.replace('-', ' ').replace(':', ' ').split()
    for t, d in zip(['hours', 'minutes', 'seconds'], delta):
        delta_dict[t] = parse_info_tree(d)
    return timedelta(**delta_dict)


def parse_info_tree(info_tree):
    if isinstance(info_tree, dict):
        return {k: parse_info_tree(v) for k, v in info_tree.items()}
    if isinstance(info_tree, list):
        return [parse_info_tree(i) for i in info_tree]
    if isinstance(info_tree, tuple):
        return tuple(parse_info_tree(i) for i in info_tree)
    
    if isinstance(info_tree, str):
        info_str = info_tree.strip()
    if '=' in info_str and ',' in info_str:
        d = [i.split('=') for i in info_str.split(',')]
        return {k: parse_info_tree(v) for k, v in d}
    if info_str.isdigit():
        return int(info_str)
    if (
        info_str
            .replace(':', '')
            .replace('-', '')
            .replace('.', '')
            .replace('T', '')
    ).isdigit():
        if ':' in info_str or '-' in info_str:
            try:
                return parse_datetime_str(info_str)
            except ValueError:             
                return parse_delta_str(info_str)
        if info_str.replace('.', '').isdigit():
            return float(info_str)
    if info_str[:-1].isdigit() and info_str[-1] in ('k', 'K', 'm', 'M', 'g', 'G'):
        if info_str[-1] in ('k', 'K'):
            return float(info_str[:-1]) * 1e3
        if info_str[-1] in ('m', 'M'):
            return float(info_str[:-1]) * 1e6
        if info_str[-1] in ('g', 'G'):
            return float(info_str[:-1]) * 1e9
    return info_str.strip()
    

def parse_slurm_out_error_msg(slurm_out: str):
    ...