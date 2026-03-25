# Getting started

This page builds a complete workflow from scratch.

## Installation

```bash
pip install reflow
pip install reflow[dev]   # for pytest, ruff, mypy
```

## Step 1: create a workflow

```python
from reflow import Workflow

wf = Workflow("demo")
```

A `Workflow` is the runtime entry point.  It validates the task graph,
submits work, and exposes status, cancel, and retry operations.

## Step 2: add a singleton task

```python
from reflow import RunDir

@wf.job(time="00:05:00", mem="1G")
def prepare(run_dir: RunDir = RunDir()) -> list[str]:
    return ["a.nc", "b.nc", "c.nc"]
```

A `job` produces a single task instance.  The `RunDir` parameter is
injected with the shared working directory at runtime — it never
appears on the CLI.

## Step 3: add an array task

```python
from typing import Annotated
from reflow import Result

@wf.job(array=True, time="00:20:00", mem="4G")
def convert(item: Annotated[str, Result(step="prepare")]) -> str:
    return item.replace(".nc", ".zarr")
```

An array task creates one instance per upstream element.  Here
`prepare()` returns `list[str]` and `convert()` consumes `str`, so
reflow infers **fan-out** and submits three array elements.

## Step 4: gather results

```python
@wf.job()
def publish(paths: Annotated[list[str], Result(step="convert")]) -> str:
    return f"published {len(paths)} outputs"
```

Reflow infers **gather** — all array outputs are collected into a
single `list[str]` for the downstream singleton.

## Step 5: add a CLI and run it

```python
if __name__ == "__main__":
    wf.cli()
```

```bash
# Inspect the DAG
python flow.py dag

# See available flags
python flow.py submit --help

# Submit a run
python flow.py submit --run-dir /scratch/demo
```

## What happens on submit

1. The workflow graph is validated (missing references, cycles, type mismatches).
2. A run record is created in the shared manifest database.
3. Task specs and initial task instances are materialised.
4. A dispatch job examines which tasks are runnable and submits them
   through the configured scheduler backend.
5. Worker processes execute Python callables and write result files.
6. The dispatcher ingests results, submits downstream tasks, and
   repeats until the graph is complete.

## Running from Python instead of the CLI

```python
run = wf.submit(run_dir="/scratch/demo", start="2025-01-01", bucket="demo")
run.status()    # dict with run state + per-task breakdown
run.cancel()    # cancel active jobs
run.retry()     # resubmit failed tasks
```

## Plain vs Annotated style

For small internal workflows, plain parameters are fine — they become
CLI flags automatically:

```python
@wf.job()
def prepare(start: str, run_dir: RunDir = RunDir()) -> list[str]:
    return [start]
```

For user-facing workflows, use `Annotated` with `Param` to get help
text, short flags, and local namespacing:

```python
from typing import Annotated
from reflow import Param

@wf.job()
def prepare(
    start: Annotated[str, Param(help="Start date in ISO-8601 format")],
    model: Annotated[str, Param(help="Model name")] = "era5",
    run_dir: RunDir = RunDir(),
) -> list[str]:
    return [f"{model}:{start}"]
```
