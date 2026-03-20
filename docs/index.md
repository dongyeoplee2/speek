# speek

*Peek into your SLURM cluster — GPU availability, job status, user usage, and more.*

**speek** is a SLURM monitoring toolkit with three interfaces — a classic CLI, a lightweight one-shot viewer, and a full interactive TUI — all built to give you immediate, readable insight into your cluster's state.

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} 📊 Cluster-wide GPU overview
See GPU utilisation per model, free vs. allocated counts, and per-node states at a glance — with colour-coded bars and demand pressure indicators.
:::

:::{grid-item-card} 👤 Per-user usage tracking
Track who is running what, GPU-hours consumed, success rates, fairshare scores, and medal rankings for top GPU consumers.
:::

:::{grid-item-card} 🗂 Live job queue & history
Monitor all RUNNING and PENDING jobs cluster-wide, with priority scores, estimated wait times, and your personal queue rank.
:::

:::{grid-item-card} 🖥 Full interactive TUI
`speek-max` is a Textual-powered terminal UI with tabs, themes, log viewing, job cancellation, and an inline job submission panel.
:::
::::

---

## Getting started

Install speek directly from GitHub:

```bash
pip install git+https://github.com/dongyeoplee2/speek.git
```

Then run any of the three interfaces:

```bash
speek          # Classic Rich table view
speek-min      # Lightweight live bars
speek-max      # Full interactive TUI
```

New to speek? Start with the {doc}`installation` guide, then pick your interface in {doc}`quickstart/index`.

```{toctree}
:maxdepth: 2
:caption: Getting Started
:hidden:

installation
quickstart/index
```

```{toctree}
:maxdepth: 2
:caption: Guides
:hidden:

guides/index
```

```{toctree}
:maxdepth: 2
:caption: Reference
:hidden:

api/index
changelog
```
