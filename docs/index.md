---
title: Reflow
description: Decorator-based HPC workflow engine with Result-based data wiring, reusable Flows, and an auto-generated CLI.
---

# reflow

Define HPC workflows as decorated Python functions. Wire data between
tasks with type annotations. Submit to any scheduler with one command.

[![License](https://img.shields.io/badge/License-MIT-purple.svg)](https://raw.githubusercontent.com/freva-org/reflow/refs/heads/main/LICENSE)
[![CI](https://github.com/freva-org/reflow/actions/workflows/ci.yaml/badge.svg)](https://github.com/freva-org/reflow/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/freva-org/reflow/graph/badge.svg?token=ZoqyoUkeJw)](https://codecov.io/gh/freva-org/reflow)
[![PyPI](https://img.shields.io/pypi/v/reflow-hpc)](https://pypi.org/project/reflow-hpc)
[![Python Versions](https://img.shields.io/pypi/pyversions/reflow-hpc)](https://pypi.org/project/reflow-hpc/)

[![Works with](https://img.shields.io/badge/works_with-Slurm%20%7C%20PBS%20%7C%20LSF%20%7C%20SGE%20%7C%20Flux-teal)](schedulers.md)

```console
pip install reflow-hpc
```

## A quick taste

```python
from typing import Annotated
from reflow import Workflow, Param, Result, RunDir

wf = Workflow("etl")

@wf.job(cpus=2, time="00:10:00", mem="4G")
def extract(
    source: Annotated[str, Param(help="Input file path")],
) -> list[str]:
    """Read a data source and split it into chunks."""
    return [f"chunk_{i}" for i in range(5)]

@wf.job(array=True, cpus=4, time="01:00:00", mem="8G")
def transform(
    chunk: Annotated[str, Result(step="extract")],
) -> str:
    """Process one chunk. Runs as a parallel array job."""
    return chunk.upper()

@wf.job(time="00:05:00")
def load(
    results: Annotated[list[str], Result(step="transform")],
) -> str:
    """Collect all results."""
    return f"loaded {len(results)} items"

if __name__ == "__main__":
    wf.cli()
```

```console
$ python pipeline.py submit --run-dir /scratch/run1 --source data.csv
Created run etl-20260301-a1b2 in /scratch/run1

$ python pipeline.py status etl-20260301-a1b2
```

## How it works

1. `extract` returns a list of 5 items.
2. reflow **fans out** — it submits 5 parallel `transform` jobs, one per item.
3. reflow **gathers** — it collects all outputs into a list for `load`.
4. Dependencies, caching, and retries are handled automatically.

## Features

**Decorator-driven** — define tasks with `@wf.job()`, wire data with
`Result`, and let reflow handle the rest.

**Automatic fan-out** — return a `list`, mark the next step as `array=True`,
and reflow submits one job per item.

**Merkle-DAG caching** — re-runs skip tasks whose inputs haven't changed.

**Reusable flows** — build a library of `Flow` objects and compose them
with `wf.include(flow)`.

**Auto-generated CLI** — `wf.cli()` gives you `submit`, `status`,
`cancel`, `retry`, and more.

**Multi-scheduler** — works with Slurm, PBS, LSF, SGE, and Flux.
Write scheduler-agnostic config once.

**Local execution** — run the full pipeline on your laptop with
`wf.run_local()` for development and testing.

## Installation

```console
pip install reflow-hpc
```

Requires Python 3.10+.

## Next steps

- [Getting started](getting-started.md) — build a workflow in five minutes
- [User guide](guide.md) — tasks, parameters, data wiring, broadcast, local execution
- [Scheduler backends](schedulers.md) — configure Slurm, PBS, LSF, SGE, or Flux
- [Caching and retries](caching.md) — how the Merkle cache works
- [CLI reference](cli-reference.md) — all subcommands and flags
- [Python reference](python-reference.md) — the `submit` and `run_local` API
