# Array jobs

Use `@wf.array_job()` when you want one task instance per input item.

## Basic fan-out

```python
@wf.job()
def prepare() -> list[str]:
    return ["a.nc", "b.nc", "c.nc"]


@wf.array_job()
def convert(item: Annotated[str, Result(step="prepare")]) -> str:
    return item.replace(".nc", ".zarr")
```

This creates three `convert` instances.

## Limiting parallelism

```python
@wf.array_job(array_parallelism=20, time="01:00:00", mem="8G")
def convert(item: Annotated[str, Result(step="prepare")]) -> str:
    return item
```

## Chaining arrays

```python
@wf.array_job()
def preprocess(item: Annotated[str, Result(step="prepare")]) -> str:
    return item


@wf.array_job()
def convert(item: Annotated[str, Result(step="preprocess")]) -> str:
    return item
```

Reflow uses index-wise chaining when the types match.

## Gather after arrays

```python
@wf.job()
def publish(paths: Annotated[list[str], Result(step="convert")]) -> str:
    return f"{len(paths)} done"
```
