# Getting started

Build a complete workflow from scratch in five minutes.

## Install

```console
pip install reflow-hpc
```

## Step 1 — create a workflow

```python
# pipeline.py
from reflow import Workflow

wf = Workflow("demo")
```

## Step 2 — add a task

```python
@wf.job(time="00:05:00", mem="1G")
def prepare() -> list[str]:
    """Return a list of items to process."""
    return ["alice", "bob", "charlie"]
```

This is a **singleton** task — it runs once and returns a list.

## Step 3 — fan out into parallel jobs

```python
from typing import Annotated
from reflow import Result

@wf.job(array=True, time="00:20:00", mem="4G")
def greet(name: Annotated[str, Result(step="prepare")]) -> str:
    """Process one item. Reflow runs this once per list element."""
    return f"Hello, {name}!"
```

`Result(step="prepare")` tells reflow that `greet` depends on
`prepare`. Because `prepare` returns `list[str]` and `greet`
takes `str`, reflow infers **fan-out** — three parallel jobs.

## Step 4 — gather results

```python
@wf.job()
def summarize(
    greetings: Annotated[list[str], Result(step="greet")],
) -> str:
    """Collect all parallel outputs into one list."""
    return f"{len(greetings)} greetings sent"
```

Reflow infers **gather** — all array outputs flow into a single list.

## Step 5 — add a CLI and run

```python
if __name__ == "__main__":
    wf.cli()
```

```console
$ python pipeline.py dag
prepare -> greet -> summarize

$ python pipeline.py submit --run-dir /tmp/demo
Created run demo-20260401-a1b2 in /tmp/demo

$ python pipeline.py status demo-20260401-a1b2
```

## Run locally (no scheduler needed)

For development and testing you can run the whole pipeline
in-process — no Slurm, no PBS, no subprocesses:

```python
run = wf.run_local(run_dir="/tmp/demo")
print(run.status(as_dict=True))
```

## Run from Python

```python
# Submit to your cluster scheduler
run = wf.submit(run_dir="/tmp/demo")
run.status()
run.cancel()
run.retry()
```

## Add CLI parameters

Use `Param` to turn function arguments into CLI flags:

```python
from reflow import Param

@wf.job()
def prepare(
    count: Annotated[int, Param(help="Number of items")] = 3,
) -> list[str]:
    return [f"item_{i}" for i in range(count)]
```

```console
$ python pipeline.py submit --run-dir /tmp/demo --count 10
```

## What happens when you submit

1. The workflow graph is validated (missing steps, cycles, type mismatches).
2. A run record is created in the manifest database.
3. A dispatch job submits runnable tasks to the scheduler.
4. Workers execute your Python functions and write results.
5. The dispatcher picks up results, submits downstream tasks, and
   repeats until the graph is complete.

## Next steps

- [User guide](guide.md) — parameters, data wiring, broadcast, reusable flows
- [Scheduler backends](schedulers.md) — configure your cluster
- [Caching and retries](caching.md) — skip work that hasn't changed
