# speek

`speek` is the classic interface — a Rich-rendered table showing GPU availability, per-user usage, job counts, and upcoming release windows across all partitions.

## Basic usage

```bash
speek
```

Run once and print. The output is a formatted table per partition with GPU states, user rankings, and job summaries.

## Options

| Short | Long | Description |
|-------|------|-------------|
| `-h` | `--help` | Show help and exit |
| `-u USER` | `--user USER` | Highlight a specific user (default: yourself) |
| `-l` | `--live` | Refresh every 1 second |
| `-f FILE` | `--file FILE` | Load user info from a file |
| `-t T_AVAIL` | `--t_avail T_AVAIL` | Time window for upcoming GPU releases, e.g. `5m`, `2h`, `1d` (default: `5m`) |

## Live mode

```bash
speek -l
```

Refreshes the display every second using Rich's `Live` renderer. Press `Ctrl+C` to exit.

## Highlighting a user

```bash
speek -u alice
```

Highlights Alice's jobs and GPU usage in the output.

## Visual indicators

### Partition usage

| Icon | Utilisation |
|------|-------------|
| ☠️ | 100% |
| 🔥 | 90 – 100% |
| ❄️ | 0 – 10% |
| 🏖️ | 0% |

### User rankings

| Icon | Meaning |
|------|---------|
| 🥇🥈🥉 | Top 3 by GPU usage |
| 🚩 | Pareto line (top users = 80% of usage) |
| 👑 | Top user in a partition |
| ⏳ | Top pending user in a partition |
