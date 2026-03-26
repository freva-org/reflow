# `reflow`

Decorator-based HPC workflow engine with Result-based data wiring,
*re*usable work*fl*ows, and an auto-generated CLI.

[![License](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)
[![CI](https://github.com/freva-org/reflow/actions/workflows/ci.yaml/badge.svg)](https://github.com/freva-org/reflow/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/freva-org/reflow/graph/badge.svg?token=ZoqyoUkeJw)](https://codecov.io/gh/freva-org/reflow)
[![Docs](https://img.shields.io/badge/docs-reflow--docs.org-blue)](https://reflow-doc.org)
[![PyPI](https://img.shields.io/pypi/v/reflow)](https://pypi.org/project/reflow)
[![Python Versions](https://img.shields.io/pypi/pyversions/reflow)](https://pypi.org/project/reflow/)

[![Works with](https://img.shields.io/badge/works_with-Slurm%20%7C%20PBS%20%7C%20LSF%20%7C%20SGE%20%7C%20Flux-teal)](https://reflow-docs.org/schedulers/)


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
    ...
    return [str(f) for f in (run_dir / "input").glob("*.nc")]

@wf.job(array=True, cpus=8, time="04:00:00", mem="32G")
def compute(
    nc_file: Annotated[str, Result(step="prepare")],
    run_dir: RunDir = RunDir(),
) -> str:
    """Process a single input file (one per array element)."""
    ...
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

**Decorator-driven**: define tasks with `@wf.job()`, wire data
between them with `Result`, and let reflow handle the rest.

**Automatic fan-out**: return a `list` from a task, mark the
downstream as `array=True`, and reflow submits one array element
per item with zero boilerplate.

**Merkle-DAG caching**:  each task instance gets a content-addressed
identity.  Re-runs skip tasks whose inputs haven't changed.

**Reusable flows**: build a library of `Flow` objects and compose
them into workflows with `wf.include(flow, prefix="...")`.

**Auto-generated CLI**: `wf.cli()` produces a full argparse CLI
with `submit`, `status`, `cancel`, `retry`, `dag`, and `describe`
subcommands.  Parameters annotated with `Param` become CLI flags.

**Multi-scheduler**: works with Slurm, PBS Pro / Torque, LSF,
SGE / UGE, and Flux out of the box.  Write scheduler-agnostic config
and reflow translates to the right flags.


## Installation

```console
$ pip install reflow
```

Requires Python 3.10+.  The only runtime dependency is
[tomli](https://pypi.org/project/tomli/) on Python 3.10 (stdlib
`tomllib` is used on 3.11+).


## Scheduler backends

Reflow supports five workload managers.  Set the backend in your
config file (`~/.config/reflow/config.toml`) or via the `REFLOW_MODE`
environment variable:

| Backend | `mode` value | Submit | Cancel | Status |
|---------|-------------|--------|--------|--------|
| Slurm | `sbatch` (default) | `sbatch` | `scancel` | `sacct` |
| PBS Pro / Torque | `qsub-pbs` | `qsub` | `qdel` | `qstat` |
| LSF | `bsub` | `bsub` | `bkill` | `bjobs` |
| SGE / UGE | `qsub-sge` | `qsub` | `qdel` | `qstat` |
| Flux | `flux` | `flux submit` | `flux cancel` | `flux jobs` |

Use `dry-run` as the mode to log commands without submitting.

### Scheduler-agnostic config

You never need to know which scheduler is active.  Use either
`partition` or `queue`, `reflow` maps them automatically:

```toml
# ~/.config/reflow/config.toml
[executor]
mode = "qsub-pbs"          # or "sbatch", "bsub", "qsub-sge", "flux"

[executor.submit_options]
partition = "compute"       # → -q compute on PBS, --partition compute on Slurm
account  = "my_project"    # → -A my_project on PBS, --account my_project on Slurm
```

Or set the executor at submit time:

```python
# Config-driven (reads mode from config/env)
run = wf.submit(run_dir="/scratch/run1", start="2025-01-01")

# Explicit shorthand
run = wf.submit(run_dir="/scratch/run1", start="2025-01-01", executor="pbs")

# Explicit instance with custom paths
from reflow import PBSExecutor
exc = PBSExecutor(qsub="/opt/pbs/bin/qsub", array_flag="-t")
run = wf.submit(run_dir="/scratch/run1", start="2025-01-01", executor=exc)
```

### Environment variables

All config values can be overridden with `REFLOW_*` environment
variables.  The most common ones:

| Variable | Effect |
|----------|--------|
| `REFLOW_MODE` | Scheduler backend (`sbatch`, `qsub-pbs`, `bsub`, …) |
| `REFLOW_PYTHON` | Python interpreter for worker jobs |
| `REFLOW_PARTITION` | Default partition / queue |
| `REFLOW_ACCOUNT` | Default account / project |


## Core concepts

### Tasks

A task is a Python function decorated with `@wf.job()`.  Parameters
control scheduler resources:

```python
@wf.job(
    cpus=4,                  # cores per task
    time="02:00:00",         # walltime
    mem="16G",               # memory
    array=True,              # run as an array job
    array_parallelism=10,    # max concurrent array elements
    cache=True,              # enable Merkle-DAG caching (default)
    version="2",             # bump to invalidate cache
    after=["cleanup"],       # explicit ordering dependency
    partition="gpu",         # scheduler-agnostic: works on all backends
)
def my_task(...) -> ...:
    ...
```

### Data wiring with Result

`Result` declares that a parameter receives output from an upstream
task.  Reflow resolves it at dispatch time:

```python
@wf.job()
def step_a() -> list[str]:
    return ["file1.nc", "file2.nc"]

@wf.job(array=True)
def step_b(
    nc_file: Annotated[str, Result(step="step_a")],
) -> str:
    # Each array element gets one item from step_a's list.
    ...
```

Wire modes are inferred from types:

| Upstream | Downstream | Mode |
|----------|-----------|------|
| `T` (singleton) | `T` (singleton) | Direct |
| `list[T]` (singleton) | `T` (array) | Fan-out — one element per array slot |
| `T` (array) | `list[T]` (singleton) | Gather — collect all into a list |
| `list[T]` (array) | `list[T]` (singleton) | Gather + flatten |

### CLI parameters with Param

```python
@wf.job()
def ingest(
    start: Annotated[str, Param(help="Start date, ISO-8601")],
    model: Annotated[Literal["era5", "icon"], Param(help="Model")] = "era5",
    chunk: Annotated[int, Param(help="Chunk size", namespace="local")] = 256,
) -> list[str]:
    ...
```

This generates:

```console
$ python pipeline.py submit --help
  --start START          Start date, ISO-8601 (required)
  --model {era5,icon}    Model (default: era5)
  --ingest-chunk CHUNK   Chunk size (default: 256)
```

`namespace="local"` prefixes the flag with the task name to avoid
collisions.

### RunDir

Parameters typed as `RunDir` (or named `run_dir`) receive a
`pathlib.Path` pointing to the shared working directory.  They
never appear on the CLI.

### Reusable Flows

Build libraries of task groups and compose them:

```python
from reflow import Flow

preprocessing = Flow("preprocess")

@preprocessing.job()
def download(...) -> list[str]: ...

@preprocessing.job(array=True)
def convert(item: Annotated[str, Result(step="download")]) -> str: ...

# In your workflow:
wf = Workflow("experiment")
wf.include(preprocessing, prefix="pre")
# Tasks become "pre.download", "pre.convert"
```


## CLI reference

```console
$ python pipeline.py submit    --run-dir DIR [--param VALUE ...]
$ python pipeline.py status    --run-id ID
$ python pipeline.py cancel    --run-id ID [--task NAME]
$ python pipeline.py retry     --run-id ID [--task NAME]
$ python pipeline.py dag       # print task dependency graph
$ python pipeline.py describe  # JSON workflow manifest
$ python pipeline.py runs      # list all runs
```


## Python API

```python
run = wf.submit(
    run_dir="/scratch/run1",
    start="2025-01-01",
    executor="pbs",         # or "lsf", "sge", "flux", "local"
    force=True,             # skip cache, re-run everything
    force_tasks=["step_a"], # skip cache for specific tasks only
    verify=True,            # verify cached Path outputs still exist
)

run.status()          # dict with run state + per-task breakdown
run.cancel()          # cancel all active jobs
run.retry()           # resubmit failed/cancelled tasks
```


## Configuration

Run `python -c "from reflow import ensure_config_exists; ensure_config_exists()"` to
generate a fully commented config at `~/.config/reflow/config.toml`.

Key sections:

```toml
[executor]
mode = "sbatch"             # scheduler backend
python = "/path/to/python"  # interpreter for workers

[executor.submit_options]
partition = "compute"       # or queue = "batch" — both work
account = "my_project"
signal = "B:INT@60"         # pre-termination signal

[notifications]
mail_user = "you@example.org"
mail_type = "FAIL"

[dispatch]
cpus = 1
time = "00:10:00"
mem = "1G"

[defaults]
run_dir = "/scratch/$USER/reflow"
```


## Development

```console
git clone https://github.com/FREVA-CLINT/reflow
cd reflow
pip install -e ".[dev]"
tox -e test
tox -e lint
tox -e types
```


## License

MIT — see [LICENSE](LICENSE) for details.
