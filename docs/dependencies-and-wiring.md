# Dependencies and data wiring

Reflow supports two different kinds of dependency:

1. **ordering only** — a task must run after another task
2. **data wiring** — a task consumes the upstream task output

## Ordering only with `after=[...]`

```python
@wf.job()
def prepare() -> str:
    return "prepared"


@wf.job(after=["prepare"])
def cleanup() -> str:
    return "done"
```

This creates a dependency without passing any data.

## Data wiring with `Result(...)`

```python
from typing import Annotated
from reflow import Result


@wf.job()
def prepare() -> str:
    return "prepared"


@wf.job()
def publish(item: Annotated[str, Result(step="prepare")]) -> str:
    return item.upper()
```

This is the main data-flow mechanism in Reflow.

## Why `Annotated` matters here

For data dependencies, Reflow needs an explicit `Result(...)` marker. That means the fully automatic “no Annotated notation” style is only suitable for plain parameters and ordering-only dependencies.

<details markdown="1">
<summary>With `Annotated` data wiring</summary>

```python
@wf.job()
def summary(paths: Annotated[list[str], Result(step="convert")]) -> str:
    return f"{len(paths)} outputs"
```
</details>

<details markdown="1">
<summary>Without `Annotated`: use explicit ordering instead</summary>

```python
@wf.job(after=["convert"])
def summary() -> str:
    return "convert must finish first"
```

This expresses scheduling, not data transfer.
</details>

## Multi-step input

`Result(steps=[...])` combines outputs from several upstream tasks.

```python
@wf.job()
def merge(
    item: Annotated[str, Result(steps=["prepare_a", "prepare_b"])],
) -> str:
    return item
```

## Type-driven wiring

Reflow inspects type hints to decide whether to:

- pass a single value directly
- fan out a list into an array job
- gather array outputs into a list
- flatten lists when appropriate

That means these pairs are important:

| Upstream return type | Downstream param type | Effect |
|---|---|---|
| `str` | `str` | direct |
| `list[str]` | `str` in `array_job` | fan-out |
| `str` from `array_job` | `list[str]` in `job` | gather |
| `list[str]` from `array_job` | `list[str]` in `job` | gather + flatten |
