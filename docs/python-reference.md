# Python reference

## Main objects

### `Workflow`

The runtime object. It validates the graph, submits work, dispatches runnable tasks, and exposes status, cancel, and retry helpers.

```python
wf = Workflow("pipeline")
```

### `Flow`

A reusable task collection without execution machinery.

```python
flow = Flow("shared-conversion")
wf.include(flow, prefix="cmip6")
```

### `Param`

Adds CLI metadata to a parameter.

```python
start: Annotated[str, Param(help="Start date")]
```

### `Result`

Marks a parameter as depending on an upstream task result.

```python
item: Annotated[str, Result(step="prepare")]
```

### `RunDir`

Injects the current run directory.

```python
run_dir: RunDir = RunDir()
```

## Submission API

```python
run = wf.submit(
    run_dir="/scratch/run-001",
    executor="local",
    force=False,
    force_tasks=["prepare"],
    verify=True,
    start="2026-01-01",
)
```

## Run handle

```python
run.status()
run.cancel()
run.retry()
```

## Store API

Most users do not need to instantiate a store explicitly, but you can:

```python
from reflow.stores.sqlite import SqliteStore

store = SqliteStore.default(wf.config)
store.init()
```

Or for an isolated test database:

```python
store = SqliteStore.for_run_dir("/tmp/reflow-test")
```
