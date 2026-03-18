# Job logs

`speek-max` can open and highlight job log files directly in the terminal — no need to `cat` or `tail` manually.

## Opening a log

### From My Jobs panel

Select a job with the arrow keys and press `l`. speek fetches the log path via `scontrol show job` (`StdOut=` field), reads the last 40 lines, and opens a scrollable modal.

### From History panel

Select a completed job and press `l`. Same behaviour — works for any job that has a readable log path recorded by SLURM.

## Log viewer

The log modal shows:

- **Border title** — job ID
- **Path bar** — full log file path
- **Content** — last 40 lines with syntax highlighting

Press `Escape` or `q` to close.

## Highlighting patterns

Lines are coloured automatically based on content:

| Pattern | Colour | Examples |
|---------|--------|---------|
| Training progress | Green | `step`, `epoch`, `loss=`, `acc=`, tqdm bars |
| Errors | Red | `Traceback`, `Error`, `CUDA out of memory`, `OOM` |
| Warnings | Yellow | `Warning`, `UserWarning` |
| W&B | Yellow | `wandb:`, `Syncing run` |

Lines that match no pattern are shown in the default terminal colour.

## When no log is found

If SLURM has no `StdOut` path recorded for the job (e.g. the job used the default log name and it has been deleted), a warning notification is shown instead of opening the modal.
