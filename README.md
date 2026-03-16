# slurm_flow

Decorator-based Slurm DAG engine with an auto-generated CLI.

Define tasks with Python decorators, submit them as Slurm jobs, and track
progress through a shared SQLAlchemy-backed manifest database.  Think of it
as a small mix of **Airflow** (DAG orchestration, state tracking) and
**Typer** (auto-generated CLI from function signatures), without the
heavy dependencies — the CLI is pure argparse.

## Features

- **Decorator API** — `@graph.job()` and `@graph.array_job()` register tasks
  and infer CLI arguments from function signatures.
- **Automatic dependency resolution** — `expand="prepare.files"` adds an
  implicit dependency on `prepare`; no need to repeat `after=`.
- **Topological dispatch** — tasks are submitted in dependency order.
- **Array job expansion** — upstream output lists are expanded into Slurm
  array jobs automatically.
- **Status / Cancel / Retry** — first-class CLI commands for inspecting runs,
  cancelling active jobs via `scancel`, and retrying failures.
- **Pluggable executors** — `SlurmExecutor` for production, `LocalExecutor`
  for testing without a cluster.
- **Multiple database backends** — SQLite (default), PostgreSQL, MariaDB.
- **Dry-run mode** — inspect generated `sbatch` commands without submitting.

## Quick start

```python
# demo_flow.py
from typing import Annotated, Dict, List
from slurm_flow import Graph, Param

graph = Graph("demo")

@graph.job(cpus=2, time="00:05:00")
def prepare(
    start: Annotated[str, Param(help="Start date, ISO-8601")],
    end: Annotated[str, Param(help="End date, ISO-8601")],
    s3_secret: Annotated[str, Param(help="Path to S3 credential file")],
    run_dir: str,
) -> Dict[str, List[str]]:
    return {"files": ["a.nc", "b.nc", "c.nc"]}

@graph.array_job(expand="prepare.files", cpus=4, time="00:10:00")
def convert(
    item: str,
    bucket: Annotated[str, Param(help="Target S3 bucket")],
    chunk_size: Annotated[int, Param(
        help="Chunk size in MB",
        namespace="local",            # → CLI flag: --convert-chunk-size
    )] = 256,
    run_dir: str = "",
) -> Dict[str, str]:
    return {"item": item, "status": "uploaded"}

@graph.job(after=["convert"])
def finalize(
    bucket: Annotated[str, Param(help="Target S3 bucket")],
    run_dir: str = "",
) -> Dict[str, str]:
    return {"status": "ok"}

if __name__ == "__main__":
    raise SystemExit(graph.cli())
```

```bash
# See the auto-generated CLI
python demo_flow.py --help

# Print the DAG
python demo_flow.py dag

# Submit (dry-run)
export SLURM_FLOW_MODE=dry-run
python demo_flow.py submit \
  --run-dir /scratch/$USER/demo-run \
  --start 2025-01-01 \
  --end 2025-01-31 \
  --s3-secret /path/to/creds.json \
  --bucket my-bucket \
  --convert-chunk-size 512

# Submit (real)
unset SLURM_FLOW_MODE
python demo_flow.py submit \
  --run-dir /scratch/$USER/demo-run \
  --start 2025-01-01 \
  --end 2025-01-31 \
  --s3-secret /path/to/creds.json \
  --bucket my-bucket
```

## Installation

```bash
pip install -e .

# With PostgreSQL driver
pip install -e .[postgresql]

# With MariaDB driver
pip install -e .[mariadb]

# With dev tools
pip install -e .[dev]
```

## CLI commands

All commands are auto-generated from task function signatures via argparse.

### `submit`

Create a new run and submit the initial dispatcher job.  CLI flags are
inferred from your task function signatures.

```bash
python demo_flow.py submit --run-dir /scratch/$USER/run1 --start 2025-01-01 --end 2025-01-31 --bucket my-bucket
```

### `status`

Show the current state of a run.

```bash
python demo_flow.py status <run-id> --run-dir /scratch/$USER/run1

# Filter by task
python demo_flow.py status <run-id> --run-dir /scratch/$USER/run1 --task convert

# JSON output for scripting
python demo_flow.py status <run-id> --run-dir /scratch/$USER/run1 --json
```

### `cancel`

Cancel active tasks.  Calls `scancel` on submitted Slurm jobs and marks
instances as `CANCELLED` in the manifest.

```bash
# Cancel everything
python demo_flow.py cancel <run-id> --run-dir /scratch/$USER/run1

# Cancel one task only
python demo_flow.py cancel <run-id> --run-dir /scratch/$USER/run1 --task convert
```

### `retry`

Mark failed or cancelled instances for retry, then re-dispatch.

```bash
# Retry all failures
python demo_flow.py retry <run-id> --run-dir /scratch/$USER/run1

# Retry one task
python demo_flow.py retry <run-id> --run-dir /scratch/$USER/run1 --task convert
```

### `runs`

List all runs in the manifest database.

```bash
python demo_flow.py runs --run-dir /scratch/$USER/run1
```

### `dag`

Print the task graph in topological order.

```bash
python demo_flow.py dag
```

### `dispatch` / `worker`

Internal commands used by Slurm jobs.  Hidden from `--help` by default.

## How it works

### 1. Submit

`submit` creates a run in the manifest database, persists task specs and
dependencies, seeds instances for root tasks, and submits a **dispatcher**
job to Slurm.

### 2. Dispatch

The dispatcher walks the DAG in topological order:

- For **singleton tasks**: if all dependencies are `SUCCESS` and the instance
  is `PENDING` or `RETRYING`, submit a worker job.
- For **array tasks**: if the upstream output is available and no instances
  exist yet, expand the output list into individual instances and submit a
  Slurm array job.  On retry, only the `RETRYING` indices are resubmitted.

### 3. Worker

A worker executes one task instance:

1. Load run parameters and task input from the manifest.
2. Build function kwargs by matching parameter names.
3. Call the decorated function.
4. Write the output (or error traceback) back to the manifest.
5. Submit a new dispatcher job so the DAG keeps progressing.

### 4. State machine

Each task instance moves through these states:

```
PENDING → SUBMITTED → RUNNING → SUCCESS
                             ↘ FAILED → RETRYING → SUBMITTED → ...
                                      ↘ CANCELLED
```

The run itself is `RUNNING` while any task is active, then transitions to
`SUCCESS` (all tasks succeeded), `FAILED` (at least one failed, none active),
or `CANCELLED` (explicit cancellation).

## Database backends

### SQLite (default)

```bash
python demo_flow.py submit --run-dir /scratch/$USER/run1 ...
```

The manifest is stored at `<run-dir>/manifest.db`.

### PostgreSQL

```bash
python demo_flow.py submit \
  --run-dir /scratch/$USER/run1 \
  --db-type postgresql \
  --db-url postgresql+psycopg://user:pass@host:5432/slurm_flow \
  ...
```

### MariaDB

```bash
python demo_flow.py submit \
  --run-dir /scratch/$USER/run1 \
  --db-type mariadb \
  --db-url mariadb+mariadbconnector://user:pass@host:3306/slurm_flow \
  ...
```

## Executor backends

### SlurmExecutor (default)

Submits jobs via `sbatch --wrap`.  Configure with environment variables:

| Variable                          | Default      | Description                    |
|-----------------------------------|-------------|--------------------------------|
| `SLURM_FLOW_MODE`                | `sbatch`    | `sbatch` or `dry-run`         |
| `SLURM_FLOW_PYTHON`              | `sys.executable` | Python interpreter for workers |
| `SLURM_FLOW_SBATCH`              | `sbatch`    | Path to `sbatch`               |
| `SLURM_FLOW_SCANCEL`             | `scancel`   | Path to `scancel`              |
| `SLURM_FLOW_SACCT`               | `sacct`     | Path to `sacct`                |
| `SLURM_FLOW_DISPATCH_PARTITION`  | `compute`   | Partition for dispatcher jobs  |
| `SLURM_FLOW_DISPATCH_ACCOUNT`    | (none)      | Account for dispatcher jobs    |

### LocalExecutor

For testing without a cluster.  Jobs run as synchronous subprocesses.

```python
from slurm_flow import Graph, LocalExecutor

graph = Graph("test")
# ... register tasks ...

graph.submit_run(
    run_dir=Path("/tmp/test-run"),
    parameters={"start": "2025-01-01"},
    executor=LocalExecutor(),
)
```

### Custom executors

Subclass `slurm_flow.Executor` and implement `submit`, `cancel`, and
`job_state`.

## Decorator API reference

### `@graph.job()`

Register a singleton task.

```python
@graph.job(
    name="my_task",        # default: function name
    cpus=4,                # default: 1
    time="02:00:00",       # default: "00:30:00"
    mem="16G",             # default: "4G"
    partition="gpu",       # default: "compute"
    account="my_project",  # default: None
    after=["other_task"],  # default: []
    extra={"gres": "gpu:1"},  # default: {}
)
def my_task(run_dir: str, start: str) -> Dict[str, Any]:
    ...
```

### `@graph.array_job()`

Register an array task that expands from upstream output.

```python
@graph.array_job(
    expand="prepare.files",    # required: "task.output_key"
    array_parallelism=16,      # optional: max concurrent tasks
    cpus=4,
    time="01:00:00",
)
def convert(item: str, bucket: str) -> Dict[str, str]:
    ...
```

The `expand` expression automatically adds the upstream task as a dependency.

### Task function rules

- Parameters are matched by name against run parameters and task input.
- Return value must be a `Dict[str, Any]` or `None`.
- The `item` parameter is reserved for array job input.
- The `run_dir` parameter receives the shared working directory.
- Use `Annotated[T, Param(...)]` for fine-grained control (see below).

## `Param` — parameter descriptors

Use `Param()` with `typing.Annotated` to control how task function
parameters appear on the auto-generated CLI.  This keeps everything in
the function signature — no separate config class needed.

### Basic usage

```python
from typing import Annotated
from slurm_flow import Graph, Param

graph = Graph("pipeline")

@graph.job()
def prepare(
    start: Annotated[str, Param(help="Start date, ISO-8601")],
    end: Annotated[str, Param(help="End date, ISO-8601")],
    run_dir: str,
) -> Dict[str, List[str]]:
    ...
```

Without `Param`, parameters still work — they just get no help text and
default to global namespace.

### Global vs. local namespace

By default every parameter is **global**: if two tasks both declare
`bucket: str`, it becomes one `--bucket` flag shared across the run.

When a parameter is task-specific, mark it as **local**:

```python
@graph.array_job(expand="prepare.files")
def convert(
    item: str,
    bucket: Annotated[str, Param(help="Target S3 bucket")],       # global
    chunk_size: Annotated[int, Param(
        help="Chunk size in MB",
        namespace="local",
    )] = 256,
) -> Dict[str, str]:
    ...
```

The resulting CLI flag is prefixed with the task name:

```bash
python flow.py submit \
    --bucket my-bucket \           # global → --bucket
    --convert-chunk-size 512       # local  → --convert-chunk-size
```

Local values are stored under `__task_params__` in the manifest and
only injected into the owning task's function call.

### Short flags

```python
notification_email: Annotated[str, Param(
    short="-n",
    help="Email for completion notification",
)] = ""
```

### `Param` reference

| Argument     | Type                      | Default    | Description                           |
|-------------|---------------------------|------------|---------------------------------------|
| `help`      | `str`                     | `""`       | CLI help text                         |
| `short`     | `str` or `None`           | `None`     | Short flag (e.g. `"-n"`)              |
| `namespace` | `"global"` or `"local"`   | `"global"` | Scope: shared or task-prefixed        |

### How it works under the hood

At **submit** time, the CLI builder introspects all task signatures,
collects `Param` metadata from `Annotated` types, deduplicates globals,
and prefixes locals.  Values are stored in `runs.parameters_json` (with
local values nested under `"__task_params__"`).

At **worker** time, `_build_kwargs` resolves each function parameter by
checking task input first, then task-local params, then global params.

## Package structure

```
slurm_flow/
├── __init__.py          # public API exports
├── _types.py            # TaskState, RunState enums
├── params.py            # Param descriptor, introspection helpers
├── cli.py               # argparse CLI with dynamic submit command
├── db.py                # SQLAlchemy schema and queries
├── graph.py             # Graph class, decorators, dispatch, worker
└── executors/
    ├── __init__.py      # Executor ABC, JobResources
    ├── slurm.py         # SlurmExecutor
    └── local.py         # LocalExecutor
```

## Design choices

**Why argparse?**  Zero extra dependencies.  The `submit` command is built
dynamically from task function signatures and `Annotated[T, Param(...)]`
metadata.  This gives us help text, type validation, and full control
without pulling in typer or click.

**Why `Annotated` + `Param`?**  It keeps parameter metadata in the function
signature where it belongs — no separate config class, no magic globals.
Bare `start: str` still works; `Param(...)` is opt-in for help text,
short flags, or local namespacing.

**Why a dispatcher after every worker?**  It is the simplest way to make the
DAG progress without a long-running daemon.  Each worker submits a lightweight
dispatcher job that checks what is ready next.

**Why an abstract Executor?**  So the same DAG definition works on Slurm,
locally, and eventually on other workload managers (PBS, LSF, Kubernetes).

**Why SQLAlchemy Core (not ORM)?**  Because the schema is simple and Core
gives full control over queries without the ORM's session complexity.

## Known limitations

- SQLite on some HPC shared filesystems can be fragile under contention.
- No task timeout enforcement (relies on Slurm's `--time`).
- No built-in notification system (email, Slack).
- Array expansion supports only one `upstream_task.output_key` expression.
- No UI yet (a Jupyter widget is planned).

## Roadmap

1. Jupyter status widget with live refresh
2. PBS / LSF executor backends
3. Task-level environment variable injection
4. Conditional branches (`@graph.job(skip_if=...)`)
5. Built-in notification hooks
6. Connection pooling tuning for PostgreSQL under heavy array workloads
