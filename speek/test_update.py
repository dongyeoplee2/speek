from rich import print
import pandas as pd
from slurm_info import get_slurm_infos, get_sacct_format_option_str, filter_fields

# info = get_slurm_infos('20-00:00:00', user='dylee23')
# info = get_slurm_infos()
# info = get_slurm_infos('3-00:00:00', user='kwanlee', keep_jobid_batch=True)
info = get_slurm_infos('0-01:00:00', user='dylee23')
info = filter_fields(info)
print('got info')

# print([*info.items()][0])
print(info)

df = pd.DataFrame.from_dict(info, orient='index')
print(df)
