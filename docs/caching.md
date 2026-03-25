# Caching and retries

Reflow uses content-addressed hashing to avoid recomputing tasks
whose inputs haven't changed.

## How caching works

For each task instance, reflow computes an **identity hash** from:

- task name
- task version string
- direct input parameters (JSON-serialised)
- upstream output hashes (propagated through the DAG)

If a previous successful instance with the same identity exists in the
manifest store, its output is reused without submitting a scheduler
job.

## What is stored

The manifest database records:

- direct input payload
- task identity hash
- output value
- output hash
- state transitions (`PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, …)

## Cache invalidation

The simplest way to invalidate a task's cache is to bump its
`version`:

```python
@wf.job(version="2")
def prepare() -> str:
    return "changed logic"
```

Since the version is part of the identity hash, any change triggers
recomputation.

## Forcing recomputation

Skip the cache entirely for a run:

```python
run = wf.submit(run_dir="/scratch/r1", force=True, start="2026-01-01")
```

Or force-rerun only specific tasks:

```python
run = wf.submit(
    run_dir="/scratch/r1",
    force_tasks=["prepare"],
    start="2026-01-01",
)
```

## Verifying cached outputs

During a normal submit, reflow trusts the stored identity hash.
During retry — or when you explicitly pass `verify=True` — it also
checks that cached outputs are still valid:

- for `Path` outputs: checks that the file still exists on disk
- for custom verify callables: calls your function

```python
run = wf.submit(run_dir="/scratch/r1", verify=True, start="2026-01-01")
```

## Custom verification

Attach a verify function to a task:

```python
def verify_output(path: str) -> bool:
    return path.endswith(".zarr")

@wf.job(verify=verify_output)
def convert(...) -> str:
    ...
```

## Retrying failed tasks

```python
run.retry()                    # retry all failed/cancelled
run.retry(task_name="convert") # retry a specific task
```

On retry, `verify=True` is the default — reflow checks that upstream
outputs still exist before resubmitting downstream work.

From the CLI:

```bash
python flow.py retry <run-id>
python flow.py retry <run-id> --task convert
```
