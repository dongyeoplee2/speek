# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/).

## [0.0.3] - 2026-03-20

### Added

- **speek-max TUI** — full interactive terminal UI built with Textual
  - Shell command bar with autocomplete for SLURM commands, file paths, partitions, GPU types
  - 80+ base16 colour schemes from [tinted-theming/schemes](https://github.com/tinted-theming/schemes)
  - Custom YAML theme support via `~/.config/speek-max/themes/`
  - My Jobs panel with Current/History tabs, project grouping, fold/unfold
  - Events panel with Unread/Read/All tabs, read state persistence, relaunch for failed jobs
  - Stats tab with GPU usage sparkline charts, hover info, breakdown tables, issue timeline
  - Job details modal with 4 panes (Detail, Output, GPU, Priority) and job navigation
  - Table sort (click header or `s`) and filter (`/`) for all tables
  - Settings panel with save/reset, SLURM toggles, refresh rates, highlight durations, time format
  - System Info tab with SLURM probe results, latency, cache file management
  - Cluster bar with colour-coded utilisation bars, demand pressure, node ranges
  - Draggable panel divider
  - Event watcher with fresh event highlighting
  - Shell supports general commands (git, python, ls, cd, etc.) with history and user aliases
  - Persistent settings, command history, and read state across sessions
  - Neutral grey UI chrome auto-derived from theme palette
  - Time format setting (relative, absolute, or both)
- Sphinx documentation site with GitHub Pages deployment
- ruff linter/formatter configuration with Google-style docstrings

### Changed

- Renamed `check_slurm_resource.py` to `speek_classic.py`
- Renamed `check_slurm_resource_light.py` to `speek_min.py`
- `sacct` history query includes `Submit` field for correct pending job timestamps

### Removed

- Inline submit widget (replaced by shell command bar)
- Config widget (replaced by settings panel)

## [0.0.2] - 2024-10-01

### Added

- Initial release of `speek` (classic) and `speek-min`
- Rich-based table rendering with per-partition GPU stats
- Per-user GPU usage rankings with medal emojis
- Live refresh mode (`-l` flag)
- Upcoming GPU release time window (`-t` flag)
