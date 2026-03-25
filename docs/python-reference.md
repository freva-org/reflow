# Python reference

## Main imports

```python
from reflow import (
    Workflow,
    Flow,
    Run,
    Param,
    Result,
    RunDir,
    # Executors
    SlurmExecutor,
    PBSExecutor,
    LSFExecutor,
    SGEExecutor,
    FluxExecutor,
    LocalExecutor,
    # Low-level
    Executor,
    JobResources,
    Config,
    ManifestCodec,
)
```

## Workflow

The main entry point.  Validates the graph, submits work, dispatches
tasks, and exposes status / cancel / retry.

```python
wf = Workflow("pipeline")
wf = Workflow("pipeline", config=my_config)
```

Key methods:

- `wf.job(...)` — register a singleton task
- `wf.job(array=True, ...)` — register an array task
- `wf.include(flow, prefix=...)` — compose a reusable flow
- `wf.validate()` — check the graph (called automatically on submit)
- `wf.cli(argv=...)` — run the auto-generated CLI
- `wf.submit(run_dir=, ...)` — create and dispatch a run
- `wf.submit_run(run_dir, parameters, ...)` — lower-level submission (used by CLI)
- `wf.dispatch(run_id, store, run_dir, ...)` — continue dispatching
- `wf.worker(run_id, store, run_dir, task_name, ...)` — execute one task instance
- `wf.cancel_run(run_id, store, ...)` — cancel active jobs
- `wf.retry_failed(run_id, store, run_dir, ...)` — resubmit failed tasks
- `wf.run_status(run_id, store)` — get run state
- `wf.describe()` — JSON workflow manifest
- `wf.describe_typed()` — typed `WorkflowDescription` object

## Submission API

```python
run = wf.submit(
    run_dir="/scratch/run-001",
    executor="pbs",              # or "lsf", "sge", "flux", "local"
    force=False,                 # skip cache entirely
    force_tasks=["prepare"],     # skip cache for specific tasks
    verify=True,                 # verify cached Path outputs
    store=my_store,              # optional custom store
    start="2026-01-01",          # task parameters as kwargs
)
```

## Run handle

The `Run` object is returned by `wf.submit()` and provides a
user-facing control surface:

```python
run.status()          # dict with run state + per-task breakdown
run.cancel()          # cancel all active jobs
run.retry()           # resubmit failed/cancelled tasks
```

Attributes: `run.run_id`, `run.run_dir`, `run.store`, `run.workflow`.

## Flow

A reusable task collection without execution machinery:

```python
flow = Flow("shared-conversion")

@flow.job()
def step_a(...) -> ...: ...

wf.include(flow, prefix="cmip6")
```

## Store API

Most users do not need to instantiate a store, but you can:

```python
from reflow.stores.sqlite import SqliteStore

# Use the shared default database
store = SqliteStore.default(wf.config)
store.init()

# Or for an isolated test
store = SqliteStore.for_run_dir("/tmp/reflow-test")
```

## Helper functions

Public helpers in `reflow.executors.helpers`:

- `default_executor(config)` — return the right executor for the configured `mode`
- `resolve_executor(executor)` — resolve a shorthand string (`"pbs"`, `"lsf"`, …) to an instance

Public helpers in `reflow.workflow`:

- `make_run_id(workflow_name)` — generate a `<name>-<date>-<hex>` run ID
- `build_kwargs(spec, run_parameters, task_input)` — build function kwargs for a worker
- `resolve_index(explicit)` — resolve array index from env vars
