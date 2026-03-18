# Priority & scheduling

`speek-max` uses SLURM's `sprio` and `sshare` tools to surface scheduling priority information throughout the UI.

## How SLURM priority works

SLURM assigns each pending job a **total priority score** composed of several weighted factors:

| Factor | Description |
|--------|-------------|
| **Age** | How long the job has been waiting — increases over time |
| **Fairshare** | How much of the cluster allocation your account has used recently |
| **Job size** | Priority boost for larger jobs (configurable per site) |
| **QOS** | Quality-of-service tier associated with your submission |
| **Partition** | Partition-specific priority adjustment |

Jobs with higher total scores run first. A low fairshare score means your account has consumed more than its share recently, which depresses your job's priority until the usage decays.

## Where priority appears in speek-max

### Queue tab — Prio column

PENDING jobs show their `sprio` total score in the **Prio** column (highlighted in yellow). RUNNING jobs show `–`. This lets you see at a glance which pending jobs are near the top of the scheduler's queue.

### My Jobs — Rank column

Each of your PENDING jobs shows its position among all PENDING jobs in the same partition:

```
#3/47
```

This means your job is 3rd of 47 pending in that partition, sorted by total priority. The rank updates every 5 seconds.

### Cluster bar — Demand pressure (↑N)

Next to each GPU model's free/total count, `speek-max` shows the number of PENDING jobs requesting that model:

```
A100-80GB  |████ 75%|  20/80  ↑12  4×  gpu1:4,7:9
```

- **Muted** — demand is below the number of free GPUs (low pressure)
- **Yellow** — demand equals or exceeds free GPUs
- **Red** — demand is more than twice the free GPUs (high pressure)

### Users tab — FairShare column

Each user's fairshare score from `sshare` is shown in the **FairShare** column:

- **Green** ≥ 0.5 — under-utilised, high priority
- **Yellow** ≥ 0.2 — balanced
- **Red** < 0.2 — over-utilised, reduced priority

A fairshare of 1.0 means the account has used none of its allocation recently. A score near 0.0 means the account has heavily exceeded its share.

## Priority tab

The **Priority** tab (key `3`) shows the full `sprio` breakdown for every pending job: age, fairshare, job size, QOS, and partition components side by side. This is useful for understanding exactly why one job ranks above another.
