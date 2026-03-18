# Tutorial

This page builds a small workflow from left to right.

## Step 1: create a workflow

```python
from reflow import Workflow

wf = Workflow("demo")
```

## Step 2: add a singleton job

```python
from reflow import RunDir


@wf.job(time="00:05:00", mem="1G")
def prepare(run_dir: RunDir = RunDir()) -> list[str]:
    return ["a.nc", "b.nc", "c.nc"]
```

A normal `job` produces a **single task instance**.

## Step 3: add an array job

```python
from typing import Annotated
from reflow import Result


@wf.array_job(time="00:20:00", mem="4G")
def convert(item: Annotated[str, Result(step="prepare")]) -> str:
    return item.replace(".nc", ".zarr")
```

An `array_job` creates one task instance per upstream element.

- `prepare()` returns `list[str]`
- `convert()` consumes `str`
- Reflow infers **fan-out**

## Step 4: gather array output back into one task

```python
@wf.job()
def publish(paths: Annotated[list[str], Result(step="convert")]) -> str:
    return f"published {len(paths)} outputs"
```

Now Reflow infers **gather** in the opposite direction.

## Step 5: run it

```python
if __name__ == "__main__":
    wf.cli()
```

```bash
python flow.py submit --run-dir /scratch/demo
```

## What happened internally

1. Reflow validates the graph
2. it creates a run record in the manifest database
3. it inserts task specs and initial task instances
4. it submits a dispatch job
5. the dispatch step submits runnable tasks
6. workers store outputs and state transitions back into the manifest database

## Annotated vs plain style

<details markdown="1">
<summary>Plain style for a minimal workflow</summary>

```python
from reflow import RunDir, Workflow

wf = Workflow("plain")


@wf.job()
def prepare(start: str, run_dir: RunDir = RunDir()) -> list[str]:
    return [start]
```

This already works. Plain parameters become CLI flags automatically.
</details>

<details markdown="1">
<summary>Annotated style when you want richer CLI docs</summary>

```python
from typing import Annotated
from reflow import Param, RunDir, Workflow

wf = Workflow("annotated")


@wf.job()
def prepare(
    start: Annotated[str, Param(help="Start date in ISO-8601 format")],
    model: Annotated[str, Param(help="Model name")] = "era5",
    run_dir: RunDir = RunDir(),
) -> list[str]:
    return [f"{model}:{start}"]
```

Use `Annotated[...]` when you want `help`, `short` flags, or local task parameter namespaces.
</details>
