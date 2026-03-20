# speek-max

`speek-max` is a full interactive TUI built with [Textual](https://textual.textualize.io/). It provides a persistent, tabbed interface for monitoring the entire cluster, managing your own jobs, and submitting new ones — all from a single terminal window.

## Launching

```bash
speek-max
```

Optional flags:

| Flag | Description |
|------|-------------|
| `--theme THEME` | Start with a specific theme (e.g. `dracula`, `monokai`, `amber`) |
| `--user USER` | Override the detected username |

## Layout

```
┌─ Cluster ────────────────────────────────────────────────┬─ My Jobs ──────┐
│ A100-80GB  |████ 75%|      20/80  ↑3  4× gpu1:4,7,9:11  │ 12345 train.sh │
│ H100       |██   25%|      8/32       2× h100-1:2        │ ...            │
├─[1 Queue]─[2 Nodes]─[3 Priority]─[4 Users]─[5 Config]───├─ History ──────┤
│                                                           │ 12300 COMPLETED│
│   (tab content)                                           │ ...            │
├───────────────────────────────────────────────────────────┴────────────────┤
│ Partition [gpu▼] GPUs [4] Time [1-00:00] Name [job] ×1  [Submit]          │
└─────────────────────────────────────────────────────────────────────────────┘
```

- **Top bar** — Cluster GPU overview with utilisation bars, demand indicators, and node ranges
- **Left panel** — Tabbed main content (Queue, Nodes, Priority, Users, Config)
- **Right panel** — Your jobs and history, always visible
- **Bottom bar** — Inline job submission

The divider between left and right panels is draggable.

## Keyboard shortcuts

### Global

| Key | Action |
|-----|--------|
| `1` – `5` | Switch tabs |
| `Ctrl+T` | Cycle theme |
| `q` | Quit |

### Queue tab

| Key | Action |
|-----|--------|
| `d` | Show job detail |
| `r` | Refresh |

### My Jobs panel

| Key | Action |
|-----|--------|
| `x` | Cancel selected job |
| `e` | Explain why job is pending |
| `d` | Show job detail |
| `l` | View job log |
| `r` | Refresh |

### History panel

| Key | Action |
|-----|--------|
| `i` | Show job detail |
| `l` | View job log |
| `d` / `w` / `m` | Switch lookback to 1d / 7d / 30d |
| `r` | Refresh |

## Tabs

### 1 — Queue
Cluster-wide RUNNING and PENDING jobs. Jobs with similar names are grouped into a single row showing count, total GPUs, and a compact ID range. PENDING jobs show their `sprio` priority score.

### 2 — Nodes
Per-node GPU breakdown with state indicators (IDLE, MIXED, ALLOCATED, DRAINED/DOWN).

### 3 — Priority
Detailed priority scores from `sprio` — age, fairshare, job size, QOS, and partition components — for all pending jobs.

### 4 — Users
Per-user analytics over a configurable lookback window (1d / 7d / 30d): running GPUs, pending jobs, GPU-hours, success rate, failure count, average job duration, fairshare score, and top partition.

### 5 — Config
Theme selection and display settings.

## Cluster bar

The always-visible top bar shows one row per GPU model:

```
A100-80GB  🔥  |████████ 87%|     10/80  ↑5   4×  gpu1:4,7:9
```

- **Bar** — colour-coded utilisation (green / yellow / red) with percentage inside
- **Count** — free / total GPUs
- **↑N** — number of PENDING jobs demanding this GPU model (demand pressure)
- **N×** — number of nodes
- **Ranges** — node names grouped by prefix and contiguous state, coloured per state

## Job submission

The bottom bar provides a compact submit panel:

```
Partition [gpu▼]  GPUs [4]  Time [1-00:00]  Name [job]  ×[1]  [Submit]
```

Set the repeat count (`×`) to submit multiple identical jobs at once. All submitted job IDs are reported in a single notification.

## Themes

`speek-max` ships with several themes switchable via `Ctrl+T`:

- `textual-dark` (default)
- `dracula`
- `monokai`
- `solarized-dark`
- `amber`
- and more

Start with a specific theme:

```bash
speek-max --theme dracula
```
