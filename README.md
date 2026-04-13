# 🔍 speek

**speek** is a suite of SLURM cluster monitoring tools — from quick one-shot snapshots to a full interactive TUI.

## Installation

```sh
pip install speek
```

For the latest development version:
```sh
pip install --pre speek
```

## Commands

| Command | Description |
|---------|-------------|
| `speek0` | Classic one-shot cluster overview — GPU availability, per-user usage, job status |
| `speek-` | Compact snapshot — per-model GPU bars, trends, pending pressure |
| `speek+` | Full interactive TUI — queue, nodes, users, stats, events, shell |

## speek0 — Classic Overview

```sh
speek0 [-u USER] [-f FILE] [-t T_AVAIL]
```

| Option | Description |
|--------|-------------|
| `-u USER` | Highlight a specific user (default: self) |
| `-f FILE` | User info CSV file |
| `-t T_AVAIL` | Time window for upcoming release, e.g. `5 m`, `1 h` |

Shows a table of GPU usage per partition, ranked users with `🥇🥈🥉`, utilization-colored counts, and your current jobs.

## speek- — Compact Snapshot

```sh
speek- [-u USER]
```

Per-GPU-model view with utilization bars, free/total counts, pending pressure (`⏸N`), availability trends (`↑↓`), and your running/pending jobs. Detects down nodes and shows them as DEAD.

## speek+ — Interactive TUI

```sh
speek+
```

Full-featured Textual TUI with:

- **Cluster** — speek0-style usage table (tab 1)
- **Queue** — all cluster jobs grouped by partition, foldable
- **Nodes** — per-partition node status with usage bars
- **Users** — per-user GPU usage, fairshare, per-partition breakdown
- **Stats** — GPU usage charts, per-user stacked view, issue dashboard
- **Logs** — session CLI output (not persisted)
- **Settings** — theme, refresh rates, cache management, log scanning
- **Info** — cluster probe results, scheduling factors, error detection rules
- **Help** — keybindings reference

### Features

- 70+ color themes (base16 standard)
- OOM and error detection (11 error types) with log scanning
- Job detail popup with stdout, stderr, GPU stats, analysis
- Built-in shell with tab completion, history, sbatch suggestions
- Per-job log hints in the table
- Event notifications with read/unread tracking
- Down node detection with DEAD indicators

## Requirements

- Python 3.8+
- SLURM cluster with `squeue`, `scontrol`, `sinfo`
- Optional: `sacct`, `sprio`, `sshare`, `sreport`, `scancel` for full features
- `rich` (all commands), `textual>=0.50.0` (speek+ only)
