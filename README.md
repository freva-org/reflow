# `reflow`

Decorator-based HPC workflow engine with Result-based data wiring,
*re*usable work*fl*ows, and an auto-generated CLI.

[![License](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)
[![CI](https://github.com/freva-org/reflow/actions/workflows/ci.yaml/badge.svg)](https://github.com/freva-org/reflow/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/freva-org/reflow/graph/badge.svg?token=ZoqyoUkeJw)](https://codecov.io/gh/freva-org/reflow)
[![Docs](https://img.shields.io/badge/docs-reflow--docs.org-blue)](https://www.reflow-docs.org)
[![PyPI](https://img.shields.io/pypi/v/reflow-hpc)](https://pypi.org/project/reflow-hpc)
[![Python Versions](https://img.shields.io/pypi/pyversions/reflow-hpc)](https://pypi.org/project/reflow-hpc/)

[![Works with](https://img.shields.io/badge/works_with-Slurm%20%7C%20PBS%20%7C%20LSF%20%7C%20SGE%20%7C%20Flux-teal)](https://www.reflow-docs.org/latest/schedulers/)


```console
pip install reflow-hpc
```

```python
from typing import Annotated
from reflow import Workflow, Param, Result

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
Created run etl-20260401-a1b2 in /scratch/run1

$ python pipeline.py status etl-20260401-a1b2
```

## Features

**Decorator-driven**: define tasks with `@wf.job()`, wire data
between them with `Result`, and let reflow handle the rest.

**Automatic fan-out**: return a `list` from a task, mark the
downstream as `array=True`, and reflow submits one array element
per item.

**Merkle-DAG caching**: each task gets a content-addressed identity.
Re-runs skip tasks whose inputs haven't changed.

**Broadcast mode**: pass a value to every array element without
splitting it — `Result(step="config", broadcast=True)`.

**Reusable flows**: build a library of `Flow` objects and compose
them into workflows with `wf.include(flow, prefix="...")`.

**Auto-generated CLI**: `wf.cli()` produces a full argparse CLI
with `submit`, `status`, `cancel`, `retry`, `dag`, and `describe`.

**Multi-scheduler**: works with Slurm, PBS Pro / Torque, LSF,
SGE / UGE, and Flux. Write scheduler-agnostic config
and reflow translates to the right flags.

**Local execution**: run the full pipeline on your laptop with
`wf.run_local()` — no scheduler needed.

**Typo protection**: misspell a task name in `Result(step=...)`
and reflow suggests the closest match.


## Installation

```console
pip install reflow-hpc
```

Requires Python 3.10+.


## Quick start

### Data wiring

`Result` declares that a parameter receives output from an upstream
task. Reflow infers the wiring mode from the types:

| Upstream returns | Downstream takes | Mode |
|---|---|---|
| `T` | `T` | Direct |
| `list[T]` | `T` (array job) | Fan-out |
| `T` (array job) | `list[T]` | Gather |
| `T` (array job) | `T` (array job) | Chain |

### Broadcast

Pass a whole value to every array element instead of splitting it:

```python
@wf.job()
def load_config() -> dict:
    return {"threshold": 0.5}

@wf.job(array=True)
def process(
    item: Annotated[str, Result(step="find_files")],
    config: Annotated[dict, Result(step="load_config", broadcast=True)],
) -> str:
    return f"{item}:{config['threshold']}"
```

### CLI parameters

```python
@wf.job()
def ingest(
    source: Annotated[str, Param(help="Input file")],
    limit: Annotated[int, Param(help="Max rows")] = 100,
) -> str:
    return source
```

```console
$ python pipeline.py submit --help
  --source SOURCE   Input file (required)
  --limit LIMIT     Max rows (default: 100)
```

### Local execution

Run the full pipeline in-process — no scheduler, no subprocesses:

```python
run = wf.run_local(
    run_dir="/tmp/test",
    source="data.csv",
    max_workers=4,          # parallelize array jobs
    on_error="continue",    # don't stop on first failure
)
run.status()
```

### Python API

```python
run = wf.submit(
    run_dir="/scratch/run1",
    source="data.csv",
    executor="pbs",         # or "slurm", "lsf", "sge", "flux"
    force=True,             # skip cache
)

run.status()
run.cancel()
run.retry()
```


## Scheduler backends

Set the backend in `~/.config/reflow/config.toml` or via the
`REFLOW_MODE` environment variable:

| Backend | `mode` value |
|---------|-------------|
| Slurm | `sbatch` (default) |
| PBS Pro / Torque | `qsub-pbs` |
| LSF | `bsub` |
| SGE / UGE | `qsub-sge` |
| Flux | `flux` |

Use `dry-run` to log commands without submitting.

Scheduler-agnostic config — use `partition` or `queue` and reflow
maps them to the right flags:

```toml
[executor]
mode = "qsub-pbs"

[executor.submit_options]
partition = "compute"
account  = "my_project"
```


## CLI reference

```console
$ python pipeline.py submit    --run-dir DIR [--param VALUE ...]
$ python pipeline.py status    RUN_ID
$ python pipeline.py cancel    RUN_ID [--task NAME]
$ python pipeline.py retry     RUN_ID [--task NAME]
$ python pipeline.py dag
$ python pipeline.py describe
$ python pipeline.py runs
```


## Configuration

Generate a fully commented config:

```console
python -c "from reflow import ensure_config_exists; ensure_config_exists()"
```

Key sections:

```toml
[executor]
mode = "sbatch"
python = "/path/to/python"

[executor.submit_options]
partition = "compute"
account = "my_project"

[dispatch]
cpus = 1
time = "00:10:00"
mem = "1G"
```

All values can be overridden with `REFLOW_*` environment variables
(`REFLOW_MODE`, `REFLOW_PARTITION`, `REFLOW_ACCOUNT`, etc.).


## Development

```console
git clone https://github.com/freva-org/reflow
cd reflow
pip install -e ".[dev]"
tox -e test
tox -e lint
tox -e types
```


## License

MIT — see [LICENSE](LICENSE) for details.
