# speek-max

`speek-max` is a full interactive TUI built with [Textual](https://textual.textualize.io/). It provides a persistent, tabbed interface for monitoring the cluster, managing jobs, and running SLURM commands — all from a single terminal window.

## Launching

```bash
speek-max
```

Optional flags:

| Flag | Description |
| ---- | ----------- |
| `--theme THEME` | Start with a specific theme (e.g. `onedark`, `catppuccin`, `gruvbox`) |
| `--user USER` | Override the detected username |

## Layout

```text
┌─ Cluster ────────────────────────────────────────────────┬─ My Jobs ──────┐
│ A100-80GB  |████ 75%|      20/80  ↑3  4× gpu1:4,7,9:11  │ [1 Current]    │
│ H100       |██   25%|      8/32       2× h100-1:2        │ [2 History]    │
├─[1 Queue]─[2 Nodes]─[3 Users]─[4 Stats]─[5 Settings]────├─ Events ──────┤
│                                                           │ [1][2][3]      │
│   (tab content)                                           │ Unread/Read/All│
├───────────────────────────────────────────────────────────┴────────────────┤
│ ╭─────────────────────────────────────────────────────────────╮           │
│ │ $ type command or : to focus                      ⎘ 📋 ⏎   │           │
│ ╰─────────────────────────────────────────────────────────────╯           │
│  d Details  f ⇥ Focus  : Shell  q Quit                                    │
└───────────────────────────────────────────────────────────────────────────┘
```

- **Top bar** — Cluster GPU overview with utilisation bars and demand indicators
- **Left panel** — Tabbed content (Queue, Nodes, Users, Stats, Settings, Info, Help)
- **Right panel** — My Jobs (Current + History tabs) and Events (Unread/Read/All tabs)
- **Shell** — Built-in command bar with SLURM autocomplete and file path completion
- **Footer** — Context-sensitive keyboard shortcuts

The divider between left and right panels is draggable.

## Keyboard shortcuts

### Global

| Key | Action |
| --- | ------ |
| `1`–`7` | Switch main tabs |
| `d` | View job details |
| `f` | Cycle focus between panels |
| `:` | Focus the shell command bar |
| `q` | Quit |

### Tables (all)

| Key | Action |
| --- | ------ |
| `j` / `k` | Move cursor down / up |
| `s` | Sort by column under cursor |
| `/` | Filter rows (type to search, Esc to clear) |
| Click header | Sort by that column |

### My Jobs panel

| Key | Action |
| --- | ------ |
| `d` | View job details |
| `v` | Fold/unfold project or job group |
| `x` | Cancel selected job |
| `1` / `2` | Switch Current / History tab |
| `r` | Refresh |

### Events panel

| Key | Action |
| --- | ------ |
| `d` | View job details |
| `v` | Expand/collapse group |
| `R` | Relaunch failed job |
| `1` / `2` / `3` | Switch Unread / Read / All tab |
| `Space` | Toggle read/unread |
| `r` | Refresh |

### Shell

| Key | Action |
| --- | ------ |
| `Tab` | Autocomplete (commands, flags, paths, partitions) |
| `Up` / `Down` | Navigate command history |
| `Esc` | Exit shell focus |
| `Enter` | Execute command |

## Tabs

### 1 — Queue
Cluster-wide RUNNING and PENDING jobs grouped by name. PENDING jobs show `sprio` priority scores.

### 2 — Nodes
Per-node GPU breakdown with state indicators (IDLE, MIXED, ALLOCATED, DRAINED/DOWN).

### 3 — Users
Per-user analytics: running GPUs, pending jobs, GPU-hours, success rate, fairshare scores.

### 4 — Stats
GPU usage timeline with sparkline charts, breakdown tables, and issue tracking over configurable time ranges.

### 5 — Settings
Theme selection, SLURM command toggles, feature flags, refresh rates, highlight durations, time format (relative/absolute/both), save/reset.

### 6 — Info
SLURM capability probe results, command latencies, cache file sizes with delete option.

### 7 — Help
Keyboard shortcuts and usage guide.

## Shell

The built-in shell at the bottom supports:

- All SLURM commands (`sbatch`, `scancel`, `squeue`, `scontrol`, `sinfo`, `sacct`, `sprio`, `srun`)
- General shell commands (`ls`, `cd`, `git`, `python`, `nvidia-smi`, pipes, etc.)
- **Tab completion** — context-aware: SLURM flags with descriptions, partition/node/GPU names from live data, file paths, recent sbatch scripts from job history
- **Command history** — persisted to `~/.config/speek-max/command_history.json`
- **User aliases** — define shortcuts in `~/.config/speek-max/commands.yaml`
- `exit` / `quit` to close speek-max

## Themes

80+ built-in themes from the [base16](https://github.com/chriskempson/base16/blob/main/styling.md) standard, plus 12 custom themes. Switch via Settings tab or `--theme` flag.

Popular picks: `onedark`, `catppuccin`, `gruvbox`, `nord`, `dracula`, `tokyonight`, `rosepine`, `kanagawa`, `everforest`, `solarized-dark`

### Custom YAML themes

Drop `.yaml` files into `~/.config/speek-max/themes/`:

```yaml
name: my-theme
primary: "#61afef"
secondary: "#c678dd"
accent: "#56b6c2"
background: "#282c34"
surface: "#353b45"
panel: "#3e4451"
warning: "#d19a66"
error: "#e06c75"
success: "#98c379"
dark: true
```

## Configuration files

| File | Purpose |
| ---- | ------- |
| `~/.config/speek-max/settings.json` | Saved settings |
| `~/.config/speek-max/command_history.json` | Shell command history |
| `~/.config/speek-max/commands.yaml` | User-defined command aliases |
| `~/.config/speek-max/themes/*.yaml` | Custom themes |
| `~/.config/speek/system_probe.json` | SLURM capability probe cache |
| `~/.cache/speek/history_read.json` | Event read/unread state |
