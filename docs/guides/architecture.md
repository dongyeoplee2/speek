# Architecture & Features

This document explains every feature in speek, what happens underneath, and how the pieces connect.

## Overview

speek has three interfaces sharing the same SLURM data layer:

```
┌─────────────────────────────────────────────────────────────┐
│                        User Interface                        │
│                                                              │
│   speek-              speek0               speek+            │
│   (one-shot)          (classic)            (full TUI)        │
│   Rich Panel          Rich Table           Textual App       │
└──────────┬────────────────┬────────────────────┬─────────────┘
           │                │                    │
           ▼                ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│                     SLURM Data Layer                          │
│                                                              │
│   squeue  scontrol  sacct  sinfo  sprio  sshare  scancel     │
│     │        │        │      │      │      │        │        │
│     ▼        ▼        ▼      ▼      ▼      ▼        ▼        │
│   Subprocess calls with caching (TTL-based)                  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Persistent Cache                           │
│                                                              │
│   ~/.cache/speek/oom_verdicts.json    OOM scan results       │
│   ~/.cache/speek/usage_trend.json    speek- trend history    │
│   ~/.cache/speek/history_read.json   Read/unread event state │
│   ~/.config/speek/system_probe.json  Command availability    │
│   ~/.config/speek-max/settings.json  User preferences        │
└─────────────────────────────────────────────────────────────┘
```

---

## speek- (One-Shot Cluster Snapshot)

**Command**: `speek-`

Prints a single Rich panel showing the entire cluster state and exits. Designed
for embedding in shell prompts or quick checks.

### Layout

```
╭──────────────────── speek- v0.0.3 ─────────────────────╮
│ 3090    24G ❄   |3%            |29/30       │ ▶19      │
│ A100    80G 💀  |100%          |0/8   ⏸7    │          │
│ A6000   48G 💀  |100%          |0/8   ⏸3 ↑2 │ ▶5 ⏸3   │
│ PRO6000 48G 💀  |100%          |0/8         │ ▶8       │
│ Total           |80%           |11/54       │ ▶32 ⏸3   │
╰────────────────────── dongyeoplee ──────────────────────╯
```

### Columns (left to right)

| Column | Description |
|--------|-------------|
| **Model** | GPU model name (bold, colored by usage) |
| **VRAM** | GPU memory in GB |
| **Emoji** | Usage indicator: 🏖 (0%), ❄ (<10%), 🔥 (>90%), 💀 (100%) |
| **Bar** | Visual usage bar with percentage |
| **Free/Total** | Available GPUs / Total GPUs |
| **Demand** | `⏸N` — N pending jobs across all users on this partition |
| **Trend** | `↑N` (green, N freed) / `↓N` (red, N taken) vs 5 min ago |
| **│ My Jobs** | `▶N` running, `⏸N` pending (your jobs only) |

### How it works

```
speek- invoked
    │
    ├── scontrol show node --oneliner
    │     → parse GPU models, VRAM, free/total per partition
    │
    ├── squeue --states=RUNNING,PENDING -o "%P|%b|%T"
    │     → count pending jobs per partition (all users)
    │
    ├── squeue -u $USER -o "%T|%P|%b"
    │     → count my running/pending GPUs per partition
    │
    ├── Load ~/.cache/speek/usage_trend.json
    │     → compare current usage to ~5 min ago for trend arrows
    │
    ├── Save current snapshot to trend file
    │
    └── Render Rich Panel → print → exit
```

### Trend tracking

The trend feature keeps a rolling history of GPU usage snapshots in
`~/.cache/speek/usage_trend.json`. Each `speek-` invocation appends the
current state and compares to the snapshot closest to 5 minutes ago.

- `↑2` (green) — 2 more GPUs available than 5 min ago
- `↓3` (red) — 3 fewer GPUs available than 5 min ago
- Empty — no 5-min-old snapshot yet, or no change

History is pruned to max 60 entries over 30 minutes.

---

## speek+ (Full Interactive TUI)

**Command**: `speek+`

A full-screen terminal UI built with [Textual](https://textual.textualize.io/).
Auto-refreshes every 5 seconds.

### Screen Layout

```
┌─────────────────────────────────────────────────────────────────┐
│ speek+  v0.0.3      2026-03-22  Sat  12:30:45     ▶3 ✔12 ✗2   │
├───────────────────────────────┬──────────────────────────────────┤
│  Cluster                      │  My Jobs                         │
│  ┌───────────────────────┐   │  ┌─────────────────────────────┐ │
│  │ 3090  24G  |63%  11/30│   │  │ 1 Current   2 History       │ │
│  │ A100  80G  |100%  0/8 │   │  │                             │ │
│  │ A6000 48G  |100%  0/8 │   │  │ ▶ sweep_agent    19  19G   │ │
│  │ PRO600048G |100%  0/8 │   │  │   ▶ sweep_agent  8   8G    │ │
│  │ Total      |80%  11/54│   │  │   ├── sweep_agent          │ │
│  └───────────────────────┘   │  │   └── sweep_agent          │ │
│                               │  └─────────────────────────────┘ │
│ 1 Queue  2 Nodes  3 Users    │                                   │
│ 4 Stats  5 Settings  6 Info  │  Events                           │
│ 7 Help                       │  ┌─────────────────────────────┐ │
│ ┌───────────────────────┐    │  │ 1 Unread  2 Read  3 All     │ │
│ │ │ A100              2 │    │  │                             │ │
│ │   ▶ AsyncPP     1  1G │    │  │ │ < 1h  12:30              │ │
│ │ │ 3090             19 │    │  │   ▶ sweep_agent    3   3G  │ │
│ │   ▶ sweep_agent 19 19G│    │  │   ├── sweep_agent          │ │
│ │ │ A6000             8 │    │  │   └── sweep_agent          │ │
│ │   ▶ sweep_agent  5 5G │    │  │ │ 1d  03/21                │ │
│ │   ▶ SSR_exp      3 3G │    │  │   ▶ sweep3_tail   7   7G  │ │
│ └───────────────────────┘    │  └─────────────────────────────┘ │
│                               │                                   │
│ $ type command or : to focus  │                                   │
├───────────────────────────────┴──────────────────────────────────┤
│ d Detail  r Refresh  v ▶/▼  f Focus  : Shell  q Quit            │
└─────────────────────────────────────────────────────────────────┘
```

### Header Bar

Shows app name, version, real-time clock, and job status summary:

```
speek+  v0.0.3      2026-03-22  Sat  12:30:45     ▶3 ⏸2 ☢1 OOM ✗2F ✔12C
```

| Symbol | Meaning |
|--------|---------|
| `▶ N` | N jobs running |
| `⏸ N` | N jobs pending |
| `☢ N OOM` | N jobs with OOM detected |
| `✗ NF` | N failed (unread events) |
| `⏱ NT` | N timed out (unread events) |
| `✔ NC` | N completed (unread events) |

---

### Cluster Bar

Shows per-partition GPU usage with colored bars, emoji indicators, and
pending job demand.

#### Data flow

```
scontrol show node --oneliner
    │
    ├── Parse: NodeName, Gres, CfgTRES, AllocTRES, State, Partitions
    │
    ├── Compute per-partition: Total GPUs, Used GPUs, Free GPUs
    │
    ├── Detect VRAM from CfgTRES (e.g., gres/gpu:a100:2(S:0-1))
    │
    └── Render table with usage bars
```

---

### Queue Tab

Shows all cluster jobs (all users) grouped by partition with fold/unfold.

#### Structure

```
│ A100                 2   8G        ← Partition divider (foldable)
  ▶ AsyncPP           1   1G  ▶     ← Job group (foldable if count > 1)
│ 3090                19  19G
  ▶ sweep_agent      19  19G  ▶     ← Fold to see 19 individual jobs
    ├── sweep_agent              ← Individual job
    ├── sweep_agent
    └── sweep_agent              ← Last child uses └──
│ A6000                8   8G
  ▶ sweep_agent       5   5G  ▶
  ▶ SSR_experiment    3   3G  ▶
```

#### Data flow

```
squeue -o "%i|%u|%j|%P|%b|%T|%M|%S" --states=RUNNING,PENDING
    │
    ├── Aggregate: group by (user, partition, state, name_base)
    │     name_base: strip trailing numbers (train_run_007 → train_run_)
    │
    ├── Build tree: Partition FoldGroups → Job FoldGroups → Leaf jobs
    │
    ├── Rank top-3 users by running GPUs (🥇🥈🥉)
    │
    └── rebuild(dt, ctx, tree) — unified tree engine renders all rows
```

---

### My Jobs Tab

Shows YOUR jobs with project grouping and two-level fold.

#### Current tab

```
▶ sweep_agent        35  35G  ▶3R ⏸2P    ← Project fold
  ▶ sweep_agent      19  19G  ▶          ← Job group fold
    ├── 15308                             ← Individual job
    └── 15309
  ▶ sweep_agent       8   8G  ▶
    sweep_agent        1   1G  ✔          ← Single job (no fold)
```

#### History tab

```
│ 1h 14:30                                ← Time divider (white bg)
  ▶ sweep_agent      35  35G  3C 2F       ← Project fold
    ▶ sweep_agent    19  19G  19C          ← Sub-group fold
      ├── sweep_agent                      ← Individual job
      └── sweep_agent
│ 1d  03/21                                ← Older time zone
  ▶ sweep3_tail       7   7G  5C 2T
```

Projects sorted by most recent activity (latest start or end time).

#### Data flow

```
Current tab:                          History tab:
squeue -u $USER                       sacct -S -7d -u $USER
-o "%i|%j|%P|%b|%T|%M|%S"            --format=JobID,JobName,Partition,
    │                                  Start,Elapsed,State,...
    ├── Group by project name             │
    │   (name_base similarity)            ├── Group by project × time zone
    │                                     │
    ├── Build tree:                       ├── Sort by most recent activity
    │   Project FoldGroup                 │
    │   → Job FoldGroup                   ├── Build tree:
    │   → Leaf (individual)               │   Time Divider
    │                                     │   → Project FoldGroup
    └── Fetch log hints +                 │   → Sub-group FoldGroup
        OOM scan (background)             │   → Leaf (individual)
                                          │
                                          └── OOM scan (background,
                                              cached to disk)
```

---

### Events Tab

Shows job state change events with read/unread tracking. Three sub-tabs:
Unread, Read, All.

#### Structure

```
│ < 1h  12:30                              ← Time divider
  ▶ sweep_agent   ✔  2m  3  3G  A6000     ← Event group
    ├── sweep_agent  ✔  2m  1  1G          ← Individual event
    └── sweep_agent  ✔  2m  1  1G
│ 1d  03/21
  sweep3_tail     ⏱  1d  1  1G  PRO6000   ← Single event (no fold)
```

The `E` column shows state badges — bold white symbol on colored background:

| Badge | State |
|-------|-------|
| ▶ green | Running |
| ⏸ amber | Pending |
| ✔ blue | Completed |
| ✗ red | Failed |
| ⏱ orange | Timeout |
| ☢ magenta | Out of Memory |
| ⊘ gray | Cancelled |

#### Data flow

```
sacct -S -{lookback}d --format=JobID,JobName,...
    │
    ├── Aggregate: group by (name, partition, state, time_bucket)
    │     time_bucket: 30-minute windows for temporal affinity
    │
    ├── Filter: exclude PENDING and RUNNING states
    │
    ├── Read/unread state from ~/.cache/speek/history_read.json
    │
    ├── OOM scan: check logs for OOM markers
    │     Results cached to ~/.cache/speek/oom_verdicts.json
    │     (instant on subsequent launches)
    │
    └── Build tree: Time Dividers → FoldGroups → Leaves
```

#### Event detection (EventWatcher)

```
Every 30 seconds:
    squeue -u $USER → current job states
    │
    ├── Compare to previous snapshot
    │     Job disappeared? → finished (trigger history refresh)
    │     State changed? → mark as fresh event
    │
    └── New events appear in Unread tab
```

---

### Nodes Tab

Per-node GPU status. Multi-partition nodes get fold/unfold dividers.

```
log-node01     3090     11  30  mixed            ← Single partition
│ log-node05                                     ← Multi-partition divider
  A6000          0   8  alloc
  A6000-debug    8   8  idle
```

#### Data flow

```
scontrol show node --oneliner
    │
    ├── Parse per-node: name, partitions, GPU model, free, total, state
    │
    ├── Group nodes: single-partition → flat Leaf
    │                multi-partition → FoldGroup with Leaf children
    │
    └── Color by state: green=idle, yellow=mixed, red=alloc/drain
```

---

### Users Tab

Per-user GPU usage analysis with partition breakdown fold.

```
🥇dongyeoplee  32  3  35  42h  91%  3   0.100  3090
  ├── 3090         19    19h  95%  1
  ├── A6000         8    12h  88%  2
  └── PRO6000       8    11h  100% 0
🥈jihunkim       3  6   9  8h   78%  2   0.200  A100
```

| Column | Description |
|--------|-------------|
| User | Username with rank emoji (top 3 by running GPUs) |
| GPU | Currently running GPUs |
| PD | Pending jobs |
| Jobs | Total jobs in lookback period |
| GPU·h | Total GPU-hours consumed |
| OK% | Success rate (completed / total) |
| Fail | Failed job count |
| Avg | Average job runtime |
| Fair | Fairshare score (higher = higher priority) |
| Part | Most-used partition |

#### Data flow

```
sacct -S -{lookback}d --format=User,Partition,State,Elapsed,AllocTRES
    │
    ├── Aggregate per user: running, pending, completed, failed, GPU-hours
    │
    ├── Per-partition breakdown (fold)
    │
    ├── sshare -a → fairshare scores
    │
    └── Lookback: 1d / 7d / 30d (switchable with d/w/m)
```

---

### Stats Tab

GPU usage timeseries with sparkline charts and per-group breakdown.

#### Sparkline chart

Shows GPU usage over time as a sparkline. Dimensions: Cluster, Partition,
Node, User, GPU Model.

#### Issues section

Shows job failures, timeouts, and OOM by partition and node.
Uses three data sources for OOM:
1. SLURM's `OUT_OF_MEMORY` state (from sacct)
2. Persistent OOM verdict cache (`~/.cache/speek/oom_verdicts.json`)
3. Live log scanning (for new jobs not yet in cache)

#### Per-group sparklines

Scrollable area with one sparkline per group (partition, node, or user),
sorted by GPU-hours descending.

---

### Settings Tab

Configure speek+ behavior. Sections:

- **Appearance** — theme selection (80+ base16 themes)
- **SLURM Commands** — enable/disable squeue, scontrol, sacct, sinfo
  (auto-locked if command unavailable on cluster)
- **Fine Controls** — per-feature toggles (history, issues, priority, etc.)
- **Performance** — refresh intervals for queue, nodes, history
- **Display** — time format, event lookback days

Settings saved to `~/.config/speek-max/settings.json`.

---

### Info Tab

Shows auto-detected SLURM cluster capabilities:

- **Cluster** — name, SLURM version, scheduler type, priority type
- **GPU Hardware** — per-model specs (VRAM, CPUs/GPU, RAM/GPU, node count)
- **SLURM Commands** — availability and latency of each command
- **sacct Capabilities** — available fields, log path strategy
- **Scheduling Factors** — priority weights and descriptions
- **My Scheduling State** — fairshare score, usage ratio, recovery estimate
- **Cache & Config Files** — sizes and paths of all cached data

Results from the initial probe (cached in `~/.config/speek/system_probe.json`).

---

### Help Tab

Keyboard shortcut reference with section cards. All shortcuts explained
with the key binding and description.

---

## Unified Table Engine

All DataTable widgets use a shared tree-based rebuild engine
(`widgets/foldable_table.py`).

### Tree structure

```
                    TreeNode
                   ╱    │    ╲
              Divider  Leaf  FoldGroup
                              │
                         children: [TreeNode...]
```

### How rebuild works

```
Widget._build_tree(data)     Widget._render_cell(node, collapsed, n_cols)
        │                              │
        ▼                              ▼
   List[TreeNode]              List[Text] (one per column)
        │                              │
        └────────── rebuild(dt, ctx, tree) ──────────┘
                         │
                    dt.clear()
                    for node in tree:
                        _emit(dt, ctx, node)  ← recursive
                              │
                              ├── Spacer → empty row
                              ├── Divider → │ LABEL (white bg)
                              ├── Leaf → ├── data / └── data
                              └── FoldGroup → ▶/▼ header
                                    │
                                    if not collapsed:
                                        emit children recursively
```

### Fold semantics

Two modes, selectable per FoldGroup:

| Mode | Default | Toggle adds to | Use case |
|------|---------|---------------|----------|
| COLLAPSED_SET | Open | collapsed set | Partitions, projects |
| EXPANDED_SET | Closed | expanded set | Job groups, users |

---

## OOM Detection Pipeline

```
Job completes
    │
    ▼
EventWatcher detects state change
    │
    ▼
HistoryWidget._scan_oom()
    │
    ├── Check ~/.cache/speek/oom_verdicts.json (instant)
    │     COMPLETED job in cache? → use cached result
    │     RUNNING job? → always re-scan (log growing)
    │
    ├── get_job_log_path(jid) → find slurm-{jid}.out
    │
    ├── detect_oom(path) → scan last 200 lines for:
    │     "out of memory", "oom", "killed", "cuda.*memory",
    │     "runtime.*error.*memory", exit code 137, etc.
    │
    ├── Save result to oom_verdicts.json
    │
    └── Update badge: ☢ on colored background
```

---

## Scheduling Priority System

```
sprio --format="%i %u %Y %A %F %J %P %Q"
    │
    ├── Per-job: priority = Σ(factor × weight)
    │
    ├── Factors (cluster-dependent):
    │     FairShare × 1000  ← past usage vs allocation
    │     QOS       × 1000  ← quality of service class
    │     Age       × 10    ← wait time bonus (max 7 days)
    │     Assoc     × 0     ← (disabled)
    │     JobSize   × 0     ← (disabled)
    │     Partition × 0     ← (disabled)
    │
    └── Shown in: Info tab, job detail popup, Users tab (Fair column)

sshare -a --parsable2
    │
    ├── Per-user: fairshare score (0.0 = heavy user, 1.0 = light user)
    │
    ├── effective_usage: your % of total cluster usage
    │
    ├── norm_shares: your fair allocation %
    │
    └── Recovery: usage decays with half-life (typically 7 days)
```

---

## Command Availability & Graceful Degradation

On first launch, speek+ probes all SLURM commands:

```
Probe: squeue ✓  scontrol ✓  sacct ✓  sinfo ✓  sprio ✓  sshare ✓

If sacct unavailable:
    ├── History tab → "sacct unavailable on this cluster"
    ├── Users tab → "sacct unavailable on this cluster"
    ├── Stats tab → disabled
    ├── Settings → sacct checkbox locked (disabled)
    └── Dependent features auto-disabled

Results cached in ~/.config/speek/system_probe.json
(re-probe with Ctrl+R in Info tab)
```

---

## State Symbols Reference

All job states use Unicode symbols with colored backgrounds:

```
 ▶  Running        ⏸  Pending        ✔  Completed
 ✗  Failed         ⏱  Timeout        ⊘  Cancelled
 ☢  OOM            ╳  Node Fail      ⏏  Preempted
 ⏯  Suspended      ↻  Requeued
```

Each has a unique background color for quick visual identification.

---

## File Structure

```
speek/
├── speek_min.py              speek- (one-shot viewer)
├── speek_classic.py          speek0 (classic CLI)
└── speek_max/
    ├── app.py                Main Textual app, layout, bindings
    ├── slurm.py              All SLURM subprocess calls + caching
    ├── probe.py              First-launch command availability probe
    ├── themes.py             80+ base16 themes + custom themes
    ├── color_schemes.py      Base16 palette definitions
    ├── log_scan.py           OOM detection in job output files
    ├── event_watcher.py      Background job state change detector
    ├── speek_max.scss        Global stylesheet
    ├── _utils.py             Shared helpers, state symbols, badges
    └── widgets/
        ├── foldable_table.py   Unified tree rebuild engine
        ├── cluster_bar.py      GPU usage bars
        ├── queue_widget.py     All-user job queue
        ├── node_widget.py      Per-node status
        ├── users_widget.py     Per-user analytics
        ├── my_jobs_widget.py   Personal jobs (current + history)
        ├── history_widget.py   Events with read/unread tracking
        ├── stats_widget.py     Usage charts + issues
        ├── settings_widget.py  Configuration panel
        ├── sysinfo_widget.py   Cluster capabilities + scheduling
        ├── help_widget.py      Keyboard shortcut reference
        ├── command_bar.py      Shell command bar with autocomplete
        ├── job_info_modal.py   Job detail popup (4 panes)
        ├── job_detail.py       Simple job detail modal
        ├── datatable.py        SpeekDataTable (custom DataTable)
        └── ping_tracker.py     Cell change flash animation
```
