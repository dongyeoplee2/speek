# speek

**[Documentation](https://edong6768.github.io/speek/)** | **[Changelog](CHANGELOG.md)**

**speek** lets you peek into SLURM cluster resources — GPU availability, job status, user usage, priority scores, and more.

![image](assets/screen.png)

## Features

- **Cluster overview** — GPU utilisation per model with colour-coded bars, demand pressure, and node ranges
- **Job queue** — all RUNNING/PENDING jobs with priority scores and estimated wait times
- **My Jobs** — your active jobs grouped by project, with history tab showing completed/failed jobs
- **Events** — job state-change timeline with read/unread tracking and relaunch for failed jobs
- **Node status** — per-node GPU breakdown with state indicators
- **User analytics** — GPU-hours, success rates, fairshare scores
- **Stats** — GPU usage timelines with sparkline charts and issue tracking
- **Shell** — built-in command bar with autocomplete for all SLURM commands
- **80+ themes** — base16 colour schemes + custom YAML themes
- **Persistent settings** — save preferences, command history, and read state across sessions

## Installation

### From GitHub (latest)

```sh
pip install git+https://github.com/edong6768/speek.git
```

### From a specific branch

```sh
pip install git+https://github.com/edong6768/speek.git@changes/0.0.3
```

### Development setup

```sh
git clone https://github.com/edong6768/speek.git
cd speek
uv sync          # or: pip install -e .
```

## Usage

> **Note:** Run from a node or login shell with SLURM access (`squeue`, `sinfo`, `sacct`, etc.).

### speek (classic)

```sh
speek [-u USER] [-l] [-f FILE] [-t T_AVAIL]
```

| Flag | Description |
| ---- | ----------- |
| `-u USER` | Highlight a specific user (default: self) |
| `-l` | Live refresh every 1 second |
| `-f FILE` | User info file |
| `-t T_AVAIL` | Upcoming release time window (e.g. `5m`, `1h`, `1d`) |

### speek-min

```sh
speek-min
```

Lightweight one-shot GPU availability bars.

### speek-max (TUI)

```sh
speek-max [--theme THEME] [--user USER]
```

Full interactive terminal UI with tabs, themes, shell, and job management.

| Key | Action |
| --- | ------ |
| `1`–`7` | Switch tabs (Queue, Nodes, Users, Stats, Settings, Info, Help) |
| `d` | View job details |
| `v` | Fold/unfold project groups |
| `f` | Cycle focus between panels |
| `:` | Focus the shell command bar |
| `s` | Sort table by column |
| `/` | Filter table rows |
| `q` | Quit |

## Documentation

Full documentation is available at the [GitHub Pages site](https://edong6768.github.io/speek/):

- [Installation](https://edong6768.github.io/speek/installation.html)
- [Quick Start](https://edong6768.github.io/speek/quickstart/index.html)
- [Themes Guide](https://edong6768.github.io/speek/guides/themes.html)
- [Changelog](https://edong6768.github.io/speek/changelog.html)
- [API Reference](https://edong6768.github.io/speek/api/index.html)

## License

MIT
