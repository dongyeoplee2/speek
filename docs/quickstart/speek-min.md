# speek-min

`speek-min` is a lightweight, fast cluster viewer. It renders GPU bars grouped by partition with colour-coded utilisation, per-user usage, and a quick "will it pend?" estimate — all without touching disk.

## Basic usage

```bash
speek-min
```

Prints a one-shot snapshot. Startup is near-instant since it avoids file I/O.

## Options

| Short | Long | Description |
|-------|------|-------------|
| `-h` | `--help` | Show help and exit |
| `-u USER` | `--user USER` | Highlight a specific user (default: yourself) |
| `-l` | `--live` | Refresh every 1 second |
| `-n` | `--nodes` | Show per-node GPU breakdown |

## Live mode

```bash
speek-min -l
```

Runs a live refresh loop. Press `Ctrl+C` to exit.

## GPU utilisation bars

Each partition shows a filled bar:

```
A100-80GB  |████████████  75%|      | 20/80 | gpu1:4,7,9:12
```

- Colour: green (< 50%), yellow (50 – 90%), red (> 90%)
- Percentage is rendered inside the filled section
- Available / total count shown inline
- Node ranges grouped by prefix and contiguous state

## Per-node breakdown

```bash
speek-min -n
```

Expands each row to show individual node states, coloured by IDLE / MIXED / ALLOCATED / DRAINED.
