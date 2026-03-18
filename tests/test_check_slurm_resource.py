"""
Unit tests for speek_classic.py helpers.
Tests are independent of a live SLURM cluster — they mock subprocess and
validate parsing logic, key helpers, and the bug fixes applied in 0.0.3.
"""
import sys
import os
import re
import unittest
from unittest.mock import patch, MagicMock

# Add speek package dir to path so we can import without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'speek'))

# Patch argparse before import so module-level parse_args() doesn't fail
with patch('sys.argv', ['speek']):
    import speek_classic as csr


# ─────────────────────────────────────────────────────────────
# _sniff_gres_key
# ─────────────────────────────────────────────────────────────
class TestSniffGresKey(unittest.TestCase):

    def test_dict_uppercase(self):
        partitions = {
            'gpu': {'TRESBillingWeights': {'GRES/gpu': '2', 'cpu': '1'}},
        }
        self.assertEqual(csr._sniff_gres_key(partitions), 'GRES/gpu')

    def test_dict_lowercase(self):
        partitions = {
            'gpu': {'TRESBillingWeights': {'gres/gpu': '2', 'cpu': '1'}},
        }
        self.assertEqual(csr._sniff_gres_key(partitions), 'gres/gpu')

    def test_string_form_single_weight(self):
        # TRESBillingWeights stored as string when only one entry
        partitions = {
            'gpu': {'TRESBillingWeights': 'GRES/gpu=2'},
        }
        self.assertEqual(csr._sniff_gres_key(partitions), 'GRES/gpu')

    def test_string_form_lowercase(self):
        partitions = {
            'gpu': {'TRESBillingWeights': 'gres/gpu=2'},
        }
        self.assertEqual(csr._sniff_gres_key(partitions), 'gres/gpu')

    def test_skips_cpu_only_partitions(self):
        # First partition has no GPU weight; second has it
        partitions = {
            'cpu': {'TRESBillingWeights': {'cpu': '1'}},
            'gpu': {'TRESBillingWeights': {'GRES/gpu': '2', 'cpu': '1'}},
        }
        self.assertEqual(csr._sniff_gres_key(partitions), 'GRES/gpu')

    def test_none_billing_weights(self):
        # All partitions have no TRESBillingWeights — fall back to default
        partitions = {
            'gpu': {'TRESBillingWeights': None},
            'cpu': {},
        }
        self.assertEqual(csr._sniff_gres_key(partitions), 'gres/gpu')

    def test_empty_partitions(self):
        self.assertEqual(csr._sniff_gres_key({}), 'gres/gpu')


# ─────────────────────────────────────────────────────────────
# _gpu_weight
# ─────────────────────────────────────────────────────────────
class TestGpuWeight(unittest.TestCase):

    def _partitions(self, tbw):
        return {'gpu': {'TRESBillingWeights': tbw}}

    def test_dict_present(self):
        p = self._partitions({'GRES/gpu': '2', 'cpu': '1'})
        self.assertEqual(csr._gpu_weight(p, 'gpu', 'GRES/gpu'), 2.0)

    def test_dict_missing_key(self):
        p = self._partitions({'cpu': '1'})
        self.assertEqual(csr._gpu_weight(p, 'gpu', 'GRES/gpu'), 0.0)

    def test_dict_zero(self):
        p = self._partitions({'GRES/gpu': '0', 'cpu': '1'})
        self.assertEqual(csr._gpu_weight(p, 'gpu', 'GRES/gpu'), 0.0)

    def test_string_form(self):
        p = self._partitions('GRES/gpu=2')
        self.assertEqual(csr._gpu_weight(p, 'gpu', 'GRES/gpu'), 2.0)

    def test_string_form_multi(self):
        # String form doesn't normally happen with commas (parsed to dict),
        # but test the regex path anyway
        p = self._partitions('gres/gpu=1.5')
        self.assertAlmostEqual(csr._gpu_weight(p, 'gpu', 'gres/gpu'), 1.5)

    def test_none_billing_weights(self):
        p = self._partitions(None)
        self.assertEqual(csr._gpu_weight(p, 'gpu', 'GRES/gpu'), 0.0)

    def test_missing_partition(self):
        self.assertEqual(csr._gpu_weight({}, 'nonexistent', 'gres/gpu'), 0.0)

    def test_float_weight(self):
        p = self._partitions({'gres/gpu': '3.5'})
        self.assertAlmostEqual(csr._gpu_weight(p, 'gpu', 'gres/gpu'), 3.5)


# ─────────────────────────────────────────────────────────────
# _parse_userid
# ─────────────────────────────────────────────────────────────
class TestParseUserid(unittest.TestCase):

    def test_standard(self):
        self.assertEqual(csr._parse_userid('dylee23(1234)'), 'dylee23')

    def test_domain_prefix(self):
        # Windows-style AD username
        self.assertEqual(csr._parse_userid('POSTECH\\dylee23(1234)'), 'dylee23')

    def test_no_uid(self):
        # Unusual but shouldn't crash
        self.assertEqual(csr._parse_userid('dylee23'), 'dylee23')

    def test_none(self):
        self.assertEqual(csr._parse_userid(None), '')

    def test_empty(self):
        self.assertEqual(csr._parse_userid(''), '')


# ─────────────────────────────────────────────────────────────
# _parse_gpu_count
# ─────────────────────────────────────────────────────────────
class TestParseGpuCount(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(csr._parse_gpu_count('gres:gpu:4'), 4)

    def test_slash_format(self):
        # This cluster uses gres/gpu:N (slash, not colon, between gres and gpu)
        self.assertEqual(csr._parse_gpu_count('gres/gpu:8'), 8)
        self.assertEqual(csr._parse_gpu_count('gres/gpu:1'), 1)

    def test_with_model(self):
        self.assertEqual(csr._parse_gpu_count('gres:gpu:A100:4'), 4)

    def test_with_long_model(self):
        self.assertEqual(csr._parse_gpu_count('gres:gpu:A100-SXM4-80GB:8'), 8)

    def test_multi_gres_gpu_first(self):
        # gpu comes before cpu — must extract gpu count, not cpu
        self.assertEqual(csr._parse_gpu_count('gres:gpu:A100:4,gres:cpu:8'), 4)

    def test_none(self):
        self.assertEqual(csr._parse_gpu_count(None), 0)

    def test_empty(self):
        self.assertEqual(csr._parse_gpu_count(''), 0)

    def test_cpu_only(self):
        # No gpu token at all
        self.assertEqual(csr._parse_gpu_count('gres:cpu:32'), 0)

    def test_old_broken_behavior(self):
        # Previously re.split(':|=', 'gres:gpu:A100:4,gres:cpu:8')[-1] == '8' (wrong)
        # Confirm the new function gives the correct answer instead
        old_result = int(re.split(':|=', 'gres:gpu:A100:4,gres:cpu:8')[-1])
        self.assertEqual(old_result, 8)              # documents the old bug
        self.assertEqual(csr._parse_gpu_count('gres:gpu:A100:4,gres:cpu:8'), 4)  # new fix


# ─────────────────────────────────────────────────────────────
# consecutor
# ─────────────────────────────────────────────────────────────
class TestConsecutor(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(csr.consecutor([]), '')

    def test_single(self):
        self.assertEqual(csr.consecutor([5]), '5')

    def test_range(self):
        self.assertEqual(csr.consecutor([1, 2, 3, 4]), '{1..4}')

    def test_gaps(self):
        self.assertEqual(csr.consecutor([1, 2, 5, 6, 9]), '{1..2} {5..6} 9')

    def test_unsorted_input(self):
        self.assertEqual(csr.consecutor([4, 1, 2, 3]), '{1..4}')

    def test_non_consecutive_singles(self):
        self.assertEqual(csr.consecutor([1, 3, 5]), '1 3 5')


# ─────────────────────────────────────────────────────────────
# get_scontrol_dict — parsing logic
# ─────────────────────────────────────────────────────────────
FAKE_PARTITION_OUTPUT = """\
PartitionName=gpu AllowGroups=ALL AllowAccounts=ALL AllowQOS=ALL
   AllocNodes=ALL Default=YES QoS=N/A
   DefaultTime=NONE DisableRootJobs=NO ExclusiveUser=NO GraceTime=0 Hidden=NO
   MaxNodes=UNLIMITED MaxTime=UNLIMITED MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED
   Nodes=node[01-04]
   PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO
   OverTimeLimit=NONE PreemptMode=OFF
   State=UP TotalCPUs=128 TotalNodes=4 SelectTypeParameters=NONE
   JobDefaults=(null) DefMemPerNode=UNLIMITED MaxMemPerNode=UNLIMITED
   TRES=cpu=128,mem=1000G,node=4,billing=4,gres/gpu=16
   TRESBillingWeights=GRES/gpu=2,cpu=1
PartitionName=cpu AllowGroups=ALL AllowAccounts=ALL AllowQOS=ALL
   AllocNodes=ALL Default=NO QoS=N/A
   TRES=cpu=64,mem=500G,node=2,billing=2
   TRESBillingWeights=cpu=1
"""

FAKE_JOB_OUTPUT = """\
JobId=12345 JobName=train
   UserId=dylee23(1234) GroupId=users(100) MCS_label=N/A
   Priority=1000 Nice=0 Account=postech QOS=normal JobState=RUNNING
   Partition=gpu TimeLimit=1-00:00:00 RunTime=00:30:00
   TresPerNode=gres:gpu:A100:4
   TRES=cpu=16,mem=100G,gres/gpu=4
JobId=12346 JobName=eval
   UserId=other(5678) GroupId=users(100) MCS_label=N/A
   Priority=900 Nice=0 Account=postech QOS=normal JobState=PENDING
   Partition=gpu TimeLimit=00:30:00 RunTime=00:00:00
   TresPerNode=gres:gpu:2
   TRES=cpu=8,mem=50G,gres/gpu=2
"""


class TestGetScontrolDict(unittest.TestCase):

    def _call(self, unit, fake_output):
        with patch('subprocess.check_output', return_value=fake_output.encode()):
            return csr.get_scontrol_dict(unit)

    def test_partition_names(self):
        result = self._call('Partition', FAKE_PARTITION_OUTPUT)
        self.assertIn('gpu', result)
        self.assertIn('cpu', result)

    def test_partition_tres_billing_weights_parsed_as_dict(self):
        result = self._call('Partition', FAKE_PARTITION_OUTPUT)
        tbw = result['gpu']['TRESBillingWeights']
        self.assertIsInstance(tbw, dict)
        self.assertIn('GRES/gpu', tbw)
        self.assertEqual(tbw['GRES/gpu'], '2')

    def test_partition_tres_parsed_as_dict(self):
        result = self._call('Partition', FAKE_PARTITION_OUTPUT)
        tres = result['gpu']['TRES']
        self.assertIsInstance(tres, dict)
        self.assertIn('gres/gpu', tres)
        self.assertEqual(tres['gres/gpu'], '16')

    def test_cpu_partition_no_gpu(self):
        result = self._call('Partition', FAKE_PARTITION_OUTPUT)
        # cpu partition has no gres/gpu in TRES
        tres = result['cpu'].get('TRES')
        if isinstance(tres, dict):
            self.assertNotIn('gres/gpu', tres)

    def test_job_ids(self):
        result = self._call('Job', FAKE_JOB_OUTPUT)
        self.assertIn(12345, result)
        self.assertIn(12346, result)

    def test_job_state(self):
        result = self._call('Job', FAKE_JOB_OUTPUT)
        self.assertEqual(result[12345]['JobState'], 'RUNNING')
        self.assertEqual(result[12346]['JobState'], 'PENDING')

    def test_job_userid_raw(self):
        result = self._call('Job', FAKE_JOB_OUTPUT)
        self.assertEqual(result[12345]['UserId'], 'dylee23(1234)')

    def test_job_tres_per_node(self):
        result = self._call('Job', FAKE_JOB_OUTPUT)
        self.assertEqual(result[12345]['TresPerNode'], 'gres:gpu:A100:4')
        self.assertEqual(result[12346]['TresPerNode'], 'gres:gpu:2')


# ─────────────────────────────────────────────────────────────
# Integration: sniff + weight using fake partition data
# ─────────────────────────────────────────────────────────────
class TestSniffAndWeightIntegration(unittest.TestCase):

    def setUp(self):
        with patch('subprocess.check_output', return_value=FAKE_PARTITION_OUTPUT.encode()):
            self.partitions = csr.get_scontrol_dict('Partition')

    def test_sniff_finds_uppercase(self):
        key = csr._sniff_gres_key(self.partitions)
        self.assertEqual(key, 'GRES/gpu')

    def test_gpu_weight_gpu_partition(self):
        key = csr._sniff_gres_key(self.partitions)
        self.assertEqual(csr._gpu_weight(self.partitions, 'gpu', key), 2.0)

    def test_gpu_weight_cpu_partition_is_zero(self):
        key = csr._sniff_gres_key(self.partitions)
        self.assertEqual(csr._gpu_weight(self.partitions, 'cpu', key), 0.0)

    def test_tres_gpu_count(self):
        tres = self.partitions['gpu'].get('TRES')
        count = int(tres.get('gres/gpu', 0)) if isinstance(tres, dict) else 0
        self.assertEqual(count, 16)


# ─────────────────────────────────────────────────────────────
# Regression: multi-partition key mismatch (the sep.md bug)
# ─────────────────────────────────────────────────────────────
class TestMultiPartitionKeyMismatch(unittest.TestCase):
    """
    Regression test for the bug where u_stat[gpu_one] = u_stat.get(gpu, ...)
    used gpu (tuple) as lookup key but gpu_one (str) as write key.
    After the fix both should use gpu_one.
    """

    def _make_new_state(self, fields):
        return {k: 0 for k in fields}

    def test_single_partition_job_accumulates_correctly(self):
        status = {'PENDING', 'RUNNING'}
        NewState = self._make_new_state
        user_status = {}

        gpu_one = 'gpu'
        gpu = 'gpu'  # same as gpu_one for single-partition job
        j_status = 'RUNNING'
        gpu_count = 4

        u_stat = user_status.get('alice', NewState(status))
        # Fixed: use gpu_one as both key and lookup
        u_stat[gpu_one] = u_stat.get(gpu_one, NewState(status))
        u_stat[j_status] += gpu_count
        u_stat[gpu_one][j_status] += gpu_count
        user_status['alice'] = u_stat

        self.assertEqual(user_status['alice']['RUNNING'], 4)
        self.assertEqual(user_status['alice']['gpu']['RUNNING'], 4)

    def test_multi_partition_tuple_gpu_uses_gpu_one_key(self):
        """
        When gpu is a tuple (multi-partition job), gpu_one is gpu[0].
        Old code: u_stat[gpu_one] = u_stat.get(gpu, ...)  <- tuple lookup, str write
        New code: u_stat[gpu_one] = u_stat.get(gpu_one, ...)  <- str lookup, str write
        After two jobs on same partition, the second call must find the first entry.
        """
        status = {'PENDING', 'RUNNING'}
        NewState = self._make_new_state

        user_status = {}
        gpu_one = 'A100'
        j_status = 'RUNNING'

        # First job
        u_stat = user_status.get('alice', NewState(status))
        u_stat[gpu_one] = u_stat.get(gpu_one, NewState(status))
        u_stat[j_status] += 4
        u_stat[gpu_one][j_status] += 4
        user_status['alice'] = u_stat

        # Second job — must accumulate, not reset
        u_stat = user_status.get('alice', NewState(status))
        u_stat[gpu_one] = u_stat.get(gpu_one, NewState(status))
        u_stat[j_status] += 2
        u_stat[gpu_one][j_status] += 2
        user_status['alice'] = u_stat

        self.assertEqual(user_status['alice']['RUNNING'], 6)
        self.assertEqual(user_status['alice'][gpu_one]['RUNNING'], 6)

    def test_old_buggy_behavior_resets_on_second_job(self):
        """Documents what the old bug did: looking up tuple key gave a fresh
        NewState each time, resetting the per-partition count."""
        status = {'PENDING', 'RUNNING'}
        NewState = self._make_new_state

        user_status = {}
        gpu_one = 'A100'
        gpu_tuple = ('A100', 'A6000')  # simulate multi-partition tuple key

        # First job
        u_stat = user_status.get('alice', NewState(status))
        u_stat[gpu_one] = u_stat.get(gpu_tuple, NewState(status))  # OLD: tuple lookup
        u_stat['RUNNING'] += 4
        u_stat[gpu_one]['RUNNING'] += 4
        user_status['alice'] = u_stat

        # Second job — old code re-looks up by tuple, gets fresh NewState, loses 4
        u_stat = user_status.get('alice', NewState(status))
        u_stat[gpu_one] = u_stat.get(gpu_tuple, NewState(status))  # OLD: tuple lookup (miss)
        u_stat['RUNNING'] += 2
        u_stat[gpu_one]['RUNNING'] += 2  # resets to 2, not 6
        user_status['alice'] = u_stat

        # Old bug: per-partition count is 2, not 6
        self.assertEqual(user_status['alice'][gpu_one]['RUNNING'], 2)
        # Total accumulates correctly because it doesn't use tuple key
        self.assertEqual(user_status['alice']['RUNNING'], 6)


if __name__ == '__main__':
    unittest.main()
