# Parameters and CLI

Reflow builds its CLI from task function signatures.

## Plain parameters

A plain function parameter becomes a CLI flag.

```python
@wf.job()
def ingest(source: str, limit: int = 100) -> str:
    return f"{source}:{limit}"
```

CLI:

```bash
python flow.py submit --run-dir /scratch/run --source input.nc --limit 200
```

## Rich CLI metadata with `Param`

Use `Param(...)` inside `Annotated[...]` when you want help text, a short option, or local scoping.

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

## Global vs local parameters

Global parameters appear once on the CLI. Local parameters are prefixed with the task name.

<details markdown="1">
<summary>Global parameter</summary>

```python
@wf.job()
def prepare(
    start: Annotated[str, Param(help="Start date")],
) -> str:
    return start
```

CLI flag: `--start`
</details>

<details markdown="1">
<summary>Task-local parameter</summary>

```python
@wf.job()
def convert(
    chunk: Annotated[int, Param(help="Chunk size", namespace="local")] = 256,
) -> str:
    return str(chunk)
```

CLI flag: `--convert-chunk`
</details>

## `run_dir` injection

`run_dir` is treated specially.

<details markdown="1">
<summary>By name only</summary>

```python
@wf.job()
def prepare(run_dir: str) -> str:
    return run_dir
```

Any parameter literally named `run_dir` is injected automatically.
</details>

<details markdown="1">
<summary>Explicit `RunDir` marker</summary>

```python
from reflow import RunDir


@wf.job()
def prepare(run_dir: RunDir = RunDir()) -> str:
    return str(run_dir)
```

This is clearer and is the recommended style in examples.
</details>

## Literal choices

`Literal[...]` values become CLI choices.

```python
from typing import Annotated, Literal


@wf.job()
def prepare(
    model: Annotated[Literal["era5", "icon"], Param(help="Model")],
) -> str:
    return model
```

## Practical advice

- use **plain parameters** when the workflow is small and internal
- use **Annotated + Param** when the workflow becomes user-facing
- keep parameter names stable so cached identities remain predictable
