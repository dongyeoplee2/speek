"""
Integration tests: validate speek's computed GPU statistics against squeue ground truth.

These tests run against a live SLURM cluster. They are skipped automatically if
squeue/scontrol are not available (non-SLURM environments).

Design principle: snapshot scontrol (speek's data source) and squeue (ground truth)
as close together in time as possible, then cross-validate. A small race tolerance
(RACE_TOLERANCE_GPUS) accounts for jobs that start or end between the two calls.
"""
import sys
import os
import re
import subprocess
import unittest
from collections import defaultdict
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'speek'))

# Patch argparse before import
import unittest.mock
with unittest.mock.patch('sys.argv', ['speek']):
    import speek_classic as csr

# ── helpers ────────────────────────────────────────────────────────────────────

GPU_RE = re.compile(r'gres[:/]gpu(?::[A-Za-z0-9_\-]+)?:(\d+)', re.IGNORECASE)
TBW_GPU_RE = re.compile(r'gres/gpu=([0-9.]+)', re.IGNORECASE)
TRES_GPU_RE = re.compile(r'gres/gpu=(\d+)', re.IGNORECASE)

RACE_TOLERANCE_GPUS = 16   # max GPUs that can start/end between two consecutive calls
DEFAULT_TW = timedelta(minutes=5)

def _slurm_available():
    try:
        subprocess.check_output(['squeue', '--version'], stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False

SLURM_AVAILABLE = _slurm_available()
skip_no_slurm = unittest.skipUnless(SLURM_AVAILABLE, 'SLURM not available')


def _squeue_ground_truth():
    """
    Snapshot squeue -t R to build ground-truth dicts.
    Returns:
      part_used  : {partition: gpus_running}
      user_total : {user: total_gpus_running}
      user_part  : {user: {partition: gpus_running}}
    """
    out = subprocess.check_output(
        ['squeue', '-t', 'R', '-o', '%u|%P|%b', '-h'],
        text=True, stderr=subprocess.DEVNULL
    )
    part_used  = defaultdict(int)
    user_total = defaultdict(int)
    user_part  = defaultdict(lambda: defaultdict(int))

    for ln in out.splitlines():
        if not ln.strip():
            continue
        user, part, gres = (ln.split('|') + ['', '', ''])[:3]
        m = GPU_RE.search(gres)
        if not m:
            continue
        g = int(m.group(1))
        part_used[part.strip()]          += g
        user_total[user.strip()]         += g
        user_part[user.strip()][part.strip()] += g

    return dict(part_used), dict(user_total), dict(user_part)


def _squeue_pending_ground_truth():
    """
    Snapshot squeue -t PD for pending GPU demand per partition.
    Returns {partition: gpus_pending}
    """
    out = subprocess.check_output(
        ['squeue', '-t', 'PD', '-o', '%u|%P|%b', '-h'],
        text=True, stderr=subprocess.DEVNULL
    )
    part_pending  = defaultdict(int)
    user_pending  = defaultdict(int)

    for ln in out.splitlines():
        if not ln.strip():
            continue
        user, part, gres = (ln.split('|') + ['', '', ''])[:3]
        m = GPU_RE.search(gres)
        if not m:
            continue
        g = int(m.group(1))
        part_pending[part.strip()] += g
        user_pending[user.strip()] += g

    return dict(part_pending), dict(user_pending)


def _gpu_partitions_from_scontrol(partitions):
    """Return list of partition names that have GPU billing weight > 0."""
    gres = csr._sniff_gres_key(partitions)
    return [p for p in partitions if csr._gpu_weight(partitions, p, gres) > 0.0]


def _part_total_from_scontrol(partitions, gpu_parts):
    """Return {partition: total_gpu_count} from scontrol TRES field."""
    totals = {}
    for p in gpu_parts:
        tres = partitions[p].get('TRES')
        totals[p] = int(tres.get('gres/gpu', 0)) if isinstance(tres, dict) else 0
    return totals


# ── test class ─────────────────────────────────────────────────────────────────

@skip_no_slurm
class TestStatsVsSqueue(unittest.TestCase):
    """
    Validates compute_gpu_stats() output against squeue ground truth.

    Snapshot order:
      1. scontrol show partition  )
      2. scontrol show job        ) → fed into compute_gpu_stats
      3. squeue -t R              ) → ground truth for RUNNING
      4. squeue -t PD             ) → ground truth for PENDING

    All 4 happen within a few hundred ms; RACE_TOLERANCE_GPUS absorbs any
    jobs that start/end in that window.
    """

    @classmethod
    def setUpClass(cls):
        # Snapshot everything as close together as possible
        cls.partitions = csr.get_scontrol_dict('Partition')
        cls.jobs       = csr.get_scontrol_dict('Job')
        cls.sq_part_used,  cls.sq_user_total, cls.sq_user_part = _squeue_ground_truth()
        cls.sq_part_pend, cls.sq_user_pend = _squeue_pending_ground_truth()

        cls.gpu_resource, cls.user_status, cls.user_job_status, cls.gres = \
            csr.compute_gpu_stats(cls.partitions, cls.jobs, DEFAULT_TW)

        cls.gpu_parts = _gpu_partitions_from_scontrol(cls.partitions)
        cls.part_total = _part_total_from_scontrol(cls.partitions, cls.gpu_parts)

    # ── partition totals (from scontrol TRES, independent of job data) ──────

    def test_partition_totals_match_scontrol_tres(self):
        """speek's per-partition Total must equal scontrol TRES gres/gpu count."""
        for p in self.gpu_parts:
            expected = self.part_total[p]
            actual   = self.gpu_resource.get(p, {}).get('Total', 0)
            self.assertEqual(
                actual, expected,
                f"Partition {p}: Total={actual}, scontrol TRES says {expected}"
            )

    def test_cluster_total_equals_sum_of_partition_totals(self):
        """Cluster-wide Total must equal sum of all GPU partition totals."""
        expected = sum(self.part_total.values())
        actual   = self.gpu_resource.get('Total', 0)
        self.assertEqual(actual, expected,
            f"Cluster Total={actual}, sum of partitions={expected}")

    # ── available / used (cross-validated against squeue -t R) ──────────────

    def test_per_partition_available_close_to_squeue(self):
        """
        Per-partition Available must be within RACE_TOLERANCE_GPUS of:
          scontrol_total - squeue_running
        """
        for p in self.gpu_parts:
            total    = self.part_total[p]
            sq_used  = self.sq_part_used.get(p, 0)
            expected_avail = total - sq_used

            actual_avail = self.gpu_resource.get(p, {}).get('Available', total)
            diff = abs(actual_avail - expected_avail)
            self.assertLessEqual(
                diff, RACE_TOLERANCE_GPUS,
                f"Partition {p}: speek Available={actual_avail}, "
                f"squeue-expected Available={expected_avail} "
                f"(total={total}, squeue_used={sq_used})"
            )

    def test_cluster_available_close_to_squeue(self):
        """Cluster-wide Available must be within RACE_TOLERANCE_GPUS of squeue total."""
        total_gpus = sum(self.part_total.values())
        sq_total_used = sum(self.sq_part_used.get(p, 0) for p in self.gpu_parts)
        expected_avail = total_gpus - sq_total_used

        actual_avail = self.gpu_resource.get('Available', 0)
        diff = abs(actual_avail - expected_avail)
        self.assertLessEqual(
            diff, RACE_TOLERANCE_GPUS,
            f"Cluster: speek Available={actual_avail}, "
            f"squeue-expected Available={expected_avail}"
        )

    def test_available_never_exceeds_total(self):
        """Available can never be greater than Total (no negative usage)."""
        for p in self.gpu_parts:
            avail = self.gpu_resource.get(p, {}).get('Available', 0)
            total = self.gpu_resource.get(p, {}).get('Total', 0)
            self.assertLessEqual(avail, total,
                f"Partition {p}: Available={avail} > Total={total}")
        # cluster-wide
        self.assertLessEqual(
            self.gpu_resource.get('Available', 0),
            self.gpu_resource.get('Total', 0)
        )

    def test_available_non_negative(self):
        """Available must be >= 0 for every partition and cluster-wide."""
        for p in self.gpu_parts:
            avail = self.gpu_resource.get(p, {}).get('Available', 0)
            self.assertGreaterEqual(avail, 0, f"Partition {p}: Available={avail} < 0")
        self.assertGreaterEqual(self.gpu_resource.get('Available', 0), 0)

    # ── usage percentage ────────────────────────────────────────────────────

    def test_usage_percentage_consistent_with_totals(self):
        """
        Usage% string must be consistent with the Available/Total values speek computed,
        not with some independent calculation.
        """
        for p in self.gpu_parts:
            pr = self.gpu_resource.get(p, {})
            total = pr.get('Total', 0)
            avail = pr.get('Available', 0)
            if total == 0:
                continue
            usage_str = pr.get('Usage', '0%')
            reported_pct = float(usage_str.rstrip('%'))
            expected_pct = (total - avail) / total * 100
            self.assertAlmostEqual(
                reported_pct, expected_pct, places=1,
                msg=f"Partition {p}: Usage={usage_str} inconsistent with "
                    f"Available={avail}, Total={total}"
            )

    # ── user running GPU totals ─────────────────────────────────────────────

    def test_user_running_totals_close_to_squeue(self):
        """
        Each user's RUNNING total in speek must be within RACE_TOLERANCE_GPUS
        of squeue ground truth.
        """
        all_users = set(self.user_status) | set(self.sq_user_total)
        for user in all_users:
            speek_running = self.user_status.get(user, {}).get('RUNNING', 0)
            sq_running    = self.sq_user_total.get(user, 0)
            diff = abs(speek_running - sq_running)
            self.assertLessEqual(
                diff, RACE_TOLERANCE_GPUS,
                f"User {user}: speek RUNNING={speek_running}, "
                f"squeue RUNNING={sq_running}"
            )

    def test_user_running_sum_equals_cluster_used(self):
        """Sum of all users' RUNNING GPUs must equal cluster-wide Used (Total - Available)."""
        cluster_used = self.gpu_resource['Total'] - self.gpu_resource['Available']
        user_sum = sum(v.get('RUNNING', 0) for v in self.user_status.values())
        self.assertEqual(user_sum, cluster_used,
            f"Sum of user RUNNING={user_sum} != cluster Used={cluster_used}")

    def test_user_pending_sum_consistent(self):
        """Sum of all users' PENDING GPUs must be non-negative and finite."""
        user_pend_sum = sum(v.get('PENDING', 0) for v in self.user_status.values())
        self.assertGreaterEqual(user_pend_sum, 0)

    def test_user_per_partition_running_close_to_squeue(self):
        """
        Per-partition RUNNING count for each user must be within tolerance of squeue.
        """
        all_users = set(self.user_status) | set(self.sq_user_part)
        for user in all_users:
            for p in self.gpu_parts:
                speek_val = self.user_status.get(user, {}).get(p, {})
                if isinstance(speek_val, dict):
                    speek_r = speek_val.get('RUNNING', 0)
                else:
                    speek_r = 0
                sq_r = self.sq_user_part.get(user, {}).get(p, 0)
                diff = abs(speek_r - sq_r)
                self.assertLessEqual(
                    diff, RACE_TOLERANCE_GPUS,
                    f"User {user}, partition {p}: "
                    f"speek RUNNING={speek_r}, squeue={sq_r}"
                )

    # ── user ranking order ──────────────────────────────────────────────────

    def test_user_ranking_order_matches_squeue(self):
        """
        The top-N users by RUNNING GPUs in speek must match squeue's top-N,
        allowing for ties and small race differences.
        """
        N = 5
        speek_order = sorted(
            self.user_status.items(),
            key=lambda x: x[1].get('RUNNING', 0),
            reverse=True
        )[:N]
        sq_order = sorted(
            self.sq_user_total.items(),
            key=lambda x: x[1],
            reverse=True
        )[:N]

        speek_top = [u for u, _ in speek_order]
        sq_top    = [u for u, _ in sq_order]

        # Allow up to 2 position swaps among top-N (ties, race conditions)
        mismatches = sum(1 for a, b in zip(speek_top, sq_top) if a != b)
        self.assertLessEqual(
            mismatches, 2,
            f"Top-{N} user order mismatch.\n"
            f"  speek: {speek_top}\n"
            f"  squeue: {sq_top}"
        )

    # ── GPU partitions detected ──────────────────────────────────────────────

    def test_gpu_partition_set_matches_scontrol(self):
        """
        The set of GPU partitions detected by speek must exactly match the
        set of partitions with GPU billing weight > 0 in scontrol.
        """
        speek_gpu_parts = set(
            p for p in self.gpu_resource
            if p not in {'Available', 'Total', 'Usage', 'max_user'}
            and isinstance(self.gpu_resource[p], dict)
        )
        expected = set(self.gpu_parts)
        self.assertEqual(speek_gpu_parts, expected,
            f"GPU partition sets differ.\n"
            f"  speek detected: {sorted(speek_gpu_parts)}\n"
            f"  scontrol says:  {sorted(expected)}")

    def test_no_cpu_only_partitions_in_results(self):
        """Partitions with GPU billing weight == 0 must not appear in gpu_resource."""
        cpu_parts = [
            p for p in self.partitions
            if csr._gpu_weight(self.partitions, p, self.gres) == 0.0
        ]
        for p in cpu_parts:
            self.assertNotIn(
                p, self.gpu_resource,
                f"CPU-only partition '{p}' should not appear in gpu_resource"
            )


@skip_no_slurm
class TestGresKeyAndWeightOnLiveCluster(unittest.TestCase):
    """Sanity checks on _sniff_gres_key and _gpu_weight with live scontrol data."""

    @classmethod
    def setUpClass(cls):
        cls.partitions = csr.get_scontrol_dict('Partition')
        cls.gres_key = csr._sniff_gres_key(cls.partitions)

    def test_gres_key_found(self):
        self.assertIn(self.gres_key, ('GRES/gpu', 'gres/gpu'),
            f"Unexpected gres key: {self.gres_key}")

    def test_at_least_one_gpu_partition(self):
        gpu_parts = [
            p for p in self.partitions
            if csr._gpu_weight(self.partitions, p, self.gres_key) > 0.0
        ]
        self.assertGreater(len(gpu_parts), 0, "No GPU partitions found")

    def test_weights_are_positive_floats(self):
        for p in self.partitions:
            w = csr._gpu_weight(self.partitions, p, self.gres_key)
            self.assertGreaterEqual(w, 0.0, f"Negative weight for partition {p}")
            self.assertIsInstance(w, float)

    def test_tres_gpu_counts_positive(self):
        for p, info in self.partitions.items():
            if csr._gpu_weight(self.partitions, p, self.gres_key) == 0.0:
                continue
            tres = info.get('TRES')
            count = int(tres.get('gres/gpu', 0)) if isinstance(tres, dict) else 0
            self.assertGreater(count, 0,
                f"GPU partition {p} has TRES gres/gpu=0")


if __name__ == '__main__':
    unittest.main(verbosity=2)
