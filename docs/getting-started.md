# Getting started

## Installation

```bash
pip install -e .
pip install -e .[dev]
```

## Minimal example

```python
from typing import Annotated
from reflow import Workflow, Param, Result, RunDir

wf = Workflow("demo")

@wf.job()
def prepare(
    start: Annotated[str, Param(help="Start date")],
    run_dir: RunDir = RunDir(),
) -> list[str]:
    return ["a.nc", "b.nc"]

@wf.array_job()
def convert(
    item: Annotated[str, Result(step="prepare")],
    bucket: Annotated[str, Param(help="Target bucket")],
    run_dir: RunDir = RunDir(),
) -> str:
    return f"s3://{bucket}/{item}.zarr"
```

## Run from Python

```python
run = wf.submit(run_dir="/scratch/demo", start="2025-01-01", bucket="demo")
run.status()
```

## Run from the CLI

```bash
python demo_flow.py dag
python demo_flow.py submit --help
python demo_flow.py submit \
  --run-dir /scratch/demo \
  --start 2025-01-01 \
  --bucket demo
```

## What happens on submit

1. the workflow is validated
2. a run record is created in the store
3. task specs and task instances are materialized
4. runnable tasks are dispatched through the executor
5. worker processes report outputs and state changes back into the store
6. the `Run` handle can inspect, cancel, or retry the run
