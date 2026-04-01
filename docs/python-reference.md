# Python reference

## Imports

```python
from reflow import (
    Workflow,       # main entry point
    Flow,           # reusable task group
    Run,            # handle to a submitted run
    Param,          # CLI parameter descriptor
    Result,         # data dependency descriptor
    RunDir,         # working directory marker
    Config,         # user configuration
)
```

## Workflow

### Creating a workflow

```python
wf = Workflow("my_pipeline")
```

### Registering tasks

```python
@wf.job(cpus=4, time="01:00:00", mem="8G")
def step_a(x: Annotated[str, Param(help="Input")]) -> list[str]:
    return [x, x + "_copy"]

@wf.job(array=True)
def step_b(item: Annotated[str, Result(step="step_a")]) -> str:
    return item.upper()
```

### Submitting to a scheduler

```python
run = wf.submit(
    run_dir="/scratch/run1",       # working directory
    x="hello",                     # task parameters
    executor="pbs",                # scheduler (or "slurm", "lsf", "sge", "flux")
    force=False,                   # skip cache if True
    force_tasks=["step_a"],        # force specific tasks only
    verify=True,                   # check cached files still exist
)
```

Returns a [`Run`](#run) handle.

### Running locally (no scheduler)

```python
run = wf.run_local(
    run_dir="/tmp/test",
    x="hello",                     # same parameters as submit
    max_workers=1,                 # parallelize array jobs (default: 1)
    force=False,
    on_error="stop",               # "stop" or "continue"
)
```

Executes the full DAG in-process, in topological order. Array tasks
are expanded and optionally parallelized across `max_workers` processes.
No scheduler or subprocess overhead.

With `on_error="continue"`, failed tasks are recorded and their
downstream dependants are skipped, but the run continues.

Returns a [`Run`](#run) handle.

### Validation

```python
wf.validate()    # raises ValueError for missing refs, cycles, type mismatches
```

Validation runs automatically on `submit`, `run_local`, and `cli`.

### Including flows

```python
from reflow import Flow

preprocessing = Flow("preprocess")

@preprocessing.job()
def download() -> list[str]: ...

wf.include(preprocessing, prefix="pre")
# Task becomes "pre_download"
```

### CLI

```python
if __name__ == "__main__":
    wf.cli()
```

### Describe

```python
wf.describe()          # JSON-safe dict of the workflow structure
wf.describe_typed()    # typed WorkflowDescription object
```

## Run

A `Run` is the handle returned by `submit` and `run_local`:

```python
run = wf.submit(run_dir="/tmp/r", x="hello")

run.run_id       # "my_pipeline-20260401-a1b2"
run.run_dir      # Path("/tmp/r")

run.status()     # print status to stdout
run.status(as_dict=True)   # return as dict

run.cancel()               # cancel active jobs
run.cancel(task="step_a")  # cancel one task

run.retry()                # retry failed tasks
run.retry(task="step_b")   # retry one task
```

## Executors

```python
from reflow import (
    SlurmExecutor, PBSExecutor, LSFExecutor,
    SGEExecutor, FluxExecutor, LocalExecutor,
)

# Pass to submit for explicit control:
run = wf.submit(
    run_dir="/tmp/r",
    executor=PBSExecutor(qsub="/opt/pbs/bin/qsub"),
    x="hello",
)
```

Or use the string shorthand: `executor="pbs"`, `executor="slurm"`, etc.

## Configuration

```python
from reflow import Config, load_config

config = load_config()    # reads ~/.config/reflow/config.toml
wf = Workflow("name", config=config)
```

Generate a fully commented config file:

```python
from reflow import ensure_config_exists
ensure_config_exists()    # writes ~/.config/reflow/config.toml if missing
```
