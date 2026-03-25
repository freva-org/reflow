# reflow

Decorator-based HPC workflow engine with Result-based data wiring,
*re*usable *fl*ows, and an auto-generated CLI.

```python
from reflow import Workflow, Param, Result, RunDir
from typing import Annotated

wf = Workflow("climate")

@wf.job(cpus=4, time="02:00:00", mem="16G")
def prepare(
    start: Annotated[str, Param(help="Start date")],
    run_dir: RunDir = RunDir(),
) -> list[str]:
    """Download and preprocess input files."""
    return [str(f) for f in (run_dir / "input").glob("*.nc")]

@wf.job(array=True, cpus=8, time="04:00:00", mem="32G")
def compute(
    nc_file: Annotated[str, Result(step="prepare")],
    run_dir: RunDir = RunDir(),
) -> str:
    """Process a single input file (one per array element)."""
    return str(run_dir / "output" / Path(nc_file).name)

if __name__ == "__main__":
    wf.cli()
```

```console
$ python pipeline.py submit --run-dir /scratch/run1 --start 2025-01-01
Created run climate-20250301-a1b2 in /scratch/run1

$ python pipeline.py status --run-id climate-20250301-a1b2
```

## Features

**Decorator-driven**
:   Define tasks with `@wf.job()`, wire data between them with `Result`,
    and let reflow handle submission, dependency chaining, and result collection.

**Automatic fan-out**
:   Return a `list` from a task, mark the downstream as `array=True`, and
    reflow submits one array element per item with zero boilerplate.

**Merkle-DAG caching**
:   Each task instance gets a content-addressed identity.  Re-runs skip
    tasks whose inputs haven't changed.

**Reusable flows**
:   Build a library of `Flow` objects and compose them into workflows
    with `wf.include(flow, prefix="...")`.

**Auto-generated CLI**
:   `wf.cli()` produces a full argparse CLI with `submit`, `status`,
    `cancel`, `retry`, `dag`, and `describe` subcommands.

**Multi-scheduler**
:   Works with Slurm, PBS Pro / Torque, LSF, SGE / UGE, and Flux out
    of the box.  Write scheduler-agnostic config and reflow translates
    to the right flags.

## Installation

```console
pip install reflow
```

Requires Python 3.10+.  The only runtime dependency is
[tomli](https://pypi.org/project/tomli/) on Python 3.10 (stdlib
`tomllib` is used on 3.11+).

## Next steps

- [Getting started](getting-started.md) — build a workflow from scratch in five minutes
- [User guide](guide.md) — concepts, parameters, data wiring, and array jobs
- [Scheduler backends](schedulers.md) — configure Slurm, PBS, LSF, SGE, or Flux
- [CLI reference](cli-reference.md) — all subcommands and flags
- [Python reference](python-reference.md) — submission API and run handles
- [API reference](api/index.md) — auto-generated from docstrings
