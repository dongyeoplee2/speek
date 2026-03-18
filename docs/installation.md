# Installation

## Requirements

- Python ≥ 3.8
- Access to a SLURM cluster (`squeue`, `scontrol`, `sinfo`, `sacct` must be available in `PATH`)

## From GitHub

```bash
pip install git+https://github.com/dongyeoplee2/speek.git
```

## Development setup

speek uses [uv](https://docs.astral.sh/uv/) for dependency management:

```bash
git clone https://github.com/dongyeoplee2/speek.git
cd speek
uv sync
```

To build the documentation locally:

```bash
uv sync --group docs
uv run sphinx-build docs docs/_build/html -b html
```

## Dependencies

| Package | Purpose |
|---------|---------|
| `rich` | Terminal rendering for `speek` and `speek-min` |
| `textual >= 0.50` | TUI framework for `speek-max` |
| `pandas >= 2.0` | Data aggregation in `speek-max` analytics |

## Verifying the installation

```bash
speek --help
speek-min --help
speek-max --help
```

Each command queries SLURM live, so run them from a node or login shell that has cluster access.
