# User guide

This page covers the core abstractions: tasks, parameters, data
wiring, array jobs, and reusable flows.

## Tasks

A task is a Python function decorated with `@wf.job()`.  Decorator
arguments control scheduler resources:

```python
@wf.job(
    cpus=4,                  # cores per task
    time="02:00:00",         # walltime
    mem="16G",               # memory
    array=True,              # run as an array job
    array_parallelism=10,    # max concurrent array elements
    cache=True,              # Merkle-DAG caching (default: True)
    version="2",             # bump to invalidate cache
    after=["cleanup"],       # explicit ordering dependency
    partition="gpu",         # scheduler-agnostic: works on all backends
)
def my_task(...) -> ...:
    ...
```

There are two task kinds:

**Singleton** (`@wf.job()`)
:   Produces a single task instance.

**Array** (`@wf.job(array=True)`)
:   Produces one instance per input element, submitted as a
    scheduler array job.

## Parameters

### Plain parameters

Any function parameter that is not a `RunDir` or `Result` becomes a
CLI flag automatically:

```python
@wf.job()
def ingest(source: str, limit: int = 100) -> str:
    return f"{source}:{limit}"
```

```bash
python flow.py submit --run-dir /scratch/run --source input.nc --limit 200
```

### Rich metadata with `Param`

Use `Param(...)` inside `Annotated[...]` for help text, short flags,
or local scoping:

```python
from typing import Annotated
from reflow import Param

@wf.job()
def ingest(
    source: Annotated[str, Param(help="Input file", short="-s")],
    limit: Annotated[int, Param(help="Maximum rows")] = 100,
) -> str:
    return source
```

### Global vs local parameters

By default, parameters are **global** — one shared CLI flag.  Set
`namespace="local"` to prefix the flag with the task name:

```python
@wf.job()
def convert(
    chunk: Annotated[int, Param(help="Chunk size", namespace="local")] = 256,
) -> str:
    return str(chunk)
```

This becomes `--convert-chunk` on the CLI, avoiding collisions when
multiple tasks share a parameter name.

### Literal choices

`Literal[...]` annotations become argparse choices:

```python
from typing import Literal

@wf.job()
def prepare(
    model: Annotated[Literal["era5", "icon"], Param(help="Model")],
) -> str:
    return model
```

### RunDir

Parameters typed as `RunDir` (or simply named `run_dir`) are injected
with a `pathlib.Path` pointing to the shared working directory.  They
never appear on the CLI.

```python
from reflow import RunDir

@wf.job()
def prepare(run_dir: RunDir = RunDir()) -> str:
    return str(run_dir / "output")
```

## Data wiring with Result

`Result` declares that a parameter receives output from an upstream
task.  The upstream automatically becomes a scheduling dependency.

```python
from reflow import Result

@wf.job()
def prepare() -> str:
    return "prepared"

@wf.job()
def publish(item: Annotated[str, Result(step="prepare")]) -> str:
    return item.upper()
```

### Multi-step input

Combine outputs from several upstream tasks with `steps=[...]`:

```python
@wf.job()
def merge(
    item: Annotated[str, Result(steps=["prepare_a", "prepare_b"])],
) -> str:
    return item
```

### Ordering without data

When you need scheduling order but no data transfer, use `after=[...]`:

```python
@wf.job(after=["prepare"])
def cleanup() -> str:
    return "done"
```

## Wire modes

Reflow inspects the upstream return type and the downstream parameter
type to decide how data flows between tasks:

| Upstream return | Downstream param | Mode |
|---|---|---|
| `T` (singleton) | `T` (singleton) | **Direct** — value passed as-is |
| `list[T]` (singleton) | `T` (array) | **Fan-out** — one element per array slot |
| `T` (array) | `list[T]` (singleton) | **Gather** — collect all into a list |
| `list[T]` (array) | `list[T]` (singleton) | **Gather + flatten** |
| `T` (array) | `T` (array) | **Chain** — index-wise 1:1 mapping |

### Fan-out

The most common pattern: a singleton returns a list, and the downstream
array task processes one element at a time.

```python
@wf.job()
def prepare() -> list[str]:
    return ["a.nc", "b.nc", "c.nc"]

@wf.job(array=True)
def convert(item: Annotated[str, Result(step="prepare")]) -> str:
    return item.replace(".nc", ".zarr")
```

This creates three `convert` instances.

### Gather

The reverse: collect all array outputs into a single downstream task.

```python
@wf.job()
def publish(paths: Annotated[list[str], Result(step="convert")]) -> str:
    return f"{len(paths)} done"
```

### Chain

Two array tasks with matching element types chain index-wise:

```python
@wf.job(array=True)
def preprocess(item: Annotated[str, Result(step="prepare")]) -> str:
    return item

@wf.job(array=True)
def convert(item: Annotated[str, Result(step="preprocess")]) -> str:
    return item
```

### Limiting array parallelism

```python
@wf.job(array=True, array_parallelism=20, time="01:00:00", mem="8G")
def convert(item: Annotated[str, Result(step="prepare")]) -> str:
    return item
```

## Reusable flows

A `Flow` is a reusable group of task definitions without execution
machinery.  Build libraries of flows and compose them into workflows:

```python
from reflow import Flow

preprocessing = Flow("preprocess")

@preprocessing.job()
def download(...) -> list[str]: ...

@preprocessing.job(array=True)
def convert(item: Annotated[str, Result(step="download")]) -> str: ...

# Compose into a workflow:
wf = Workflow("experiment")
wf.include(preprocessing, prefix="pre")
# Tasks become "pre.download", "pre.convert"
# Internal Result references are rewritten automatically.
```

You can include multiple flows, each with its own prefix, building
complex pipelines from tested, reusable components.

## Typed manifest serialisation

The manifest layer uses a built-in typed serialiser so values such as
`Path`, `datetime`, `UUID`, enums, tuples, and dataclasses can
round-trip cleanly through JSON storage.
