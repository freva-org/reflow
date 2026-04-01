# User guide

## Tasks

A task is a Python function decorated with `@wf.job()`:

```python
@wf.job(cpus=4, time="02:00:00", mem="16G")
def process(run_dir: RunDir = RunDir()) -> str:
    return str(run_dir / "output.txt")
```

The decorator arguments control scheduler resources. All of them are optional:

```python
@wf.job(
    cpus=4,                  # cores per task
    time="02:00:00",         # walltime limit
    mem="16G",               # memory
    array=True,              # run as a parallel array job
    array_parallelism=10,    # max concurrent array elements
    cache=True,              # Merkle-DAG caching (default)
    version="2",             # bump to invalidate cache
    after=["cleanup"],       # ordering dependency (no data)
    partition="gpu",         # scheduler-agnostic queue name
)
```

## Parameters

### Plain parameters

Any argument that isn't a `RunDir` or `Result` becomes a CLI flag:

```python
@wf.job()
def ingest(source: str, limit: int = 100) -> str:
    return f"{source}:{limit}"
```

```console
$ python pipeline.py submit --run-dir /tmp/r --source input.csv --limit 200
```

### Annotated parameters with `Param`

Add help text, short flags, or local scoping:

```python
@wf.job()
def ingest(
    source: Annotated[str, Param(help="Input file", short="-s")],
    limit: Annotated[int, Param(help="Max rows")] = 100,
) -> str:
    return source
```

### Local namespacing

When two tasks share a parameter name, use `namespace="local"` to
prefix the flag with the task name:

```python
@wf.job()
def step_a(
    chunk: Annotated[int, Param(namespace="local")] = 256,
) -> str: ...

@wf.job()
def step_b(
    chunk: Annotated[int, Param(namespace="local")] = 512,
) -> str: ...
```

This gives you `--step-a-chunk` and `--step-b-chunk` on the CLI.

### Literal choices

`Literal` annotations become argparse choices:

```python
@wf.job()
def train(
    backend: Annotated[Literal["cpu", "gpu", "tpu"], Param(help="Device")],
) -> str:
    return backend
```

### RunDir

Parameters typed as `RunDir` receive the shared working directory as a
`pathlib.Path`. They never appear on the CLI:

```python
@wf.job()
def process(run_dir: RunDir = RunDir()) -> str:
    output = run_dir / "results"
    output.mkdir(exist_ok=True)
    return str(output / "data.csv")
```

## Data wiring with `Result`

`Result` tells reflow that a parameter receives output from an upstream
task. The dependency is created automatically:

```python
@wf.job()
def step_a() -> str:
    return "hello"

@wf.job()
def step_b(msg: Annotated[str, Result(step="step_a")]) -> str:
    return msg.upper()
```

### Wire modes

Reflow looks at the types and figures out how data should flow:

| Upstream returns | Downstream takes | What happens |
|---|---|---|
| `str` | `str` | **Direct** — value passed as-is |
| `list[str]` | `str` (array job) | **Fan-out** — one element per job |
| `str` (array job) | `list[str]` | **Gather** — all outputs collected |
| `str` (array job) | `str` (array job) | **Chain** — 1:1 index mapping |

### Fan-out

The most common pattern. A singleton returns a list, and an array job
processes one element at a time:

```python
@wf.job()
def find_files() -> list[str]:
    return ["a.txt", "b.txt", "c.txt"]

@wf.job(array=True)
def process(path: Annotated[str, Result(step="find_files")]) -> str:
    return path.upper()
```

This creates three parallel `process` jobs.

### Gather

The reverse — collect all array outputs into one task:

```python
@wf.job()
def report(
    results: Annotated[list[str], Result(step="process")],
) -> str:
    return f"processed {len(results)} files"
```

### Chain

Two array jobs with matching types chain index-by-index:

```python
@wf.job(array=True)
def step_1(item: Annotated[str, Result(step="find_files")]) -> str:
    return item.lower()

@wf.job(array=True)
def step_2(item: Annotated[str, Result(step="step_1")]) -> str:
    return item.strip()
```

### Broadcast

Sometimes you want to pass a value to every element of an array job
**without splitting it**. If the types match (e.g. `list[str]` →
`list[str]`), reflow infers broadcast automatically:

```python
@wf.job()
def load_config() -> dict:
    return {"threshold": 0.5, "method": "linear"}

@wf.job()
def find_files() -> list[str]:
    return ["a.txt", "b.txt", "c.txt"]

@wf.job(array=True)
def process(
    path: Annotated[str, Result(step="find_files")],
    config: Annotated[dict, Result(step="load_config", broadcast=True)],
) -> str:
    # Every array element gets the full config dict
    return f"{path}:{config['method']}"
```

Without `broadcast=True`, reflow would try to fan out the dict and
fail. The `broadcast` flag says "pass this whole value to every
element."

Type-inferred broadcast works too — if upstream returns `list[str]`
and the downstream array parameter is also `list[str]`, reflow
broadcasts instead of fanning out:

```python
@wf.job()
def all_labels() -> list[str]:
    return ["cat", "dog", "bird"]

@wf.job(array=True)
def classify(
    image: Annotated[str, Result(step="find_images")],
    labels: Annotated[list[str], Result(step="all_labels")],
) -> str:
    # 'labels' is the full list in every array element
    return f"classified {image} into {labels}"
```

### Combining multiple upstreams

Use `steps=[...]` to concatenate outputs from several tasks:

```python
@wf.job(array=True)
def process(
    item: Annotated[str, Result(steps=["source_a", "source_b"])],
) -> str:
    return item
```

### Ordering without data

When you need "run B after A" but no data transfer:

```python
@wf.job(after=["setup"])
def compute() -> str:
    return "done"
```

### Typo protection

If you misspell a task name, reflow suggests the closest match:

```python
@wf.job(array=True)
def process(item: Annotated[str, Result(step="preprae")]) -> str:
    ...
```

```
ValueError: Task 'process' param 'item' references unknown task 'preprae'.
  Did you mean 'prepare'?
```

## Reusable flows

A `Flow` is a group of task definitions without execution machinery.
Build libraries of tested components and compose them:

```python
from reflow import Flow, Workflow

# Define a reusable flow
ingestion = Flow("ingest")

@ingestion.job()
def download() -> list[str]:
    return ["file_1", "file_2"]

@ingestion.job(array=True)
def validate(item: Annotated[str, Result(step="download")]) -> str:
    return item

# Compose into a workflow
wf = Workflow("pipeline")
wf.include(ingestion, prefix="raw")
# Tasks become "raw_download" and "raw_validate"
# Internal Result references are rewritten automatically
```

You can include multiple flows, each with its own prefix.

## Local execution

For development and testing, run the full pipeline without a scheduler:

```python
run = wf.run_local(run_dir="/tmp/test")
print(run.status(as_dict=True))
```

`run_local` executes tasks in topological order in-process. It supports
the same caching, force, and verify options as `submit`:

```python
run = wf.run_local(
    run_dir="/tmp/test",
    source="data.csv",       # same parameters as submit
    max_workers=4,           # parallelize array jobs across processes
    force=True,              # skip cache
    on_error="continue",     # don't stop on first failure
)
```

With `on_error="continue"`, failed tasks are recorded but the run
keeps going — downstream tasks that depend on failures are skipped.

## Array parallelism

Limit how many array elements run at once:

```python
@wf.job(array=True, array_parallelism=20)
def process(item: Annotated[str, Result(step="prepare")]) -> str:
    return item
```

This submits all elements but tells the scheduler to run at most 20
concurrently.
