# Caching and retries

Reflow stores task inputs, outputs, and content identities in the manifest database.

## What is cached

For each task instance, Reflow stores:

- direct input payload
- task identity hash
- output value
- output hash
- state transitions such as `PENDING`, `RUNNING`, and `SUCCESS`

## Cache invalidation

The easiest manual invalidation tool is the task `version` field.

```python
@wf.job(version="2")
def prepare() -> str:
    return "changed"
```

When the task logic changes, bump `version`.

## Forcing recomputation

```python
run = wf.submit(run_dir="/scratch/r1", force=True, start="2026-01-01")
```

Or only for selected tasks:

```python
run = wf.submit(
    run_dir="/scratch/r1",
    force_tasks=["prepare"],
    start="2026-01-01",
)
```

## Verifying cache hits

During a normal submit, Reflow trusts the stored identity.

During retry, or when you pass `verify=True`, it can also verify outputs such as file existence.

```python
run = wf.submit(run_dir="/scratch/r1", verify=True, start="2026-01-01")
```

## Custom verification

```python
def verify_output(path: str) -> bool:
    return path.endswith(".zarr")


@wf.job(verify=verify_output)
def convert(...) -> str:
    ...
```
