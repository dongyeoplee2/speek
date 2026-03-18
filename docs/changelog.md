# Changelog

## 0.0.3

- **speek-max**: Full Textual TUI with cluster bar, queue, nodes, users, my jobs, history, config, and submit panels
- **Priority features**: `sprio`/`sshare` integration — queue Prio column, cluster bar demand pressure (↑N), My Jobs queue rank (#N/M), Users FairShare column
- **Event watcher**: Background thread notifies on job state changes (started, completed, failed, timed out, cancelled)
- **Log viewer**: `LogModal` with pattern highlighting for training progress, errors, warnings, and W&B output
- **Panel layout**: Draggable divider between left tabbed content and right My Jobs + History panel
- **Inline submit**: Compact full-width submit bar with partition, GPU, time, name, and repeat count fields
- Renamed `check_slurm_resource.py` → `speek_classic.py`, `check_slurm_resource_light.py` → `speek_min.py`

## 0.0.2

- Initial release of `speek` (classic) and `speek-min`
- Rich-based table rendering with per-partition GPU stats
- Per-user GPU usage rankings with medal emojis
- Live refresh mode (`-l` flag)
- Upcoming GPU release time window (`-t` flag)
