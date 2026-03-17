# reflow

Decorator-based HPC workflow engine with an auto-generated CLI.

Define tasks with Python decorators, wire data flow with `Result`
annotations, and submit to Slurm with a CLI built from your function
signatures -- or interactively from a notebook.

Zero heavy dependencies: the CLI is pure argparse, the manifest store
is stdlib `sqlite3`.  The only optional dependency is `tomli` for
config file support on Python 3.10.

## Features

- **Decorator API** -- `@wf.job()` and `@wf.array_job()` with typed
  parameter annotations.
- **Automatic dependency inference** -- `Result(step="prepare")` adds
  the dependency *and* wires the data.
- **Static type validation** -- return types vs. parameter types are
  checked at registration time.
- **Fan-out / gather / chain** -- all data flow patterns between
  singletons and arrays.
- **Reusable flows** -- define tasks on a `Flow`, include in multiple
  workflows with optional prefixes.
- **Interactive `Run` handle** -- `wf.submit()` returns a `Run` for
  status, cancel, retry from notebooks.
- **Literal support** -- `Literal["era5", "icon"]` maps to argparse
  choices automatically.
- **Config file** -- `~/.config/reflow/config.toml` for default
  partition, account, python, server URL.
- **Pluggable executors** -- `SlurmExecutor`, `LocalExecutor`, or
  implement your own.
- **Pluggable stores** -- `SqliteStore` (stdlib), future REST server.

## Quick start (CLI)

```python
# demo_flow.py
from typing import Annotated, Literal
from reflow import Workflow, Param, Result, RunDir

wf = Workflow("demo")

@wf.job()
def prepare(
    start: Annotated[str, Param(help="Start date, ISO-8601")],
    end: Annotated[str, Param(help="End date, ISO-8601")],
    model: Annotated[Literal["era5", "icon"], Param(help="Model")] = "era5",
    run_dir: RunDir = RunDir(),
) -> list[str]:
    return ["a.nc", "b.nc", "c.nc"]

@wf.array_job(cpus=4, time="00:10:00")
def convert(
    nc_file: Annotated[str, Result(step="prepare")],
    bucket: Annotated[str, Param(help="S3 bucket")],
    run_dir: RunDir = RunDir(),
) -> str:
    return f"/data/{nc_file}.zarr"

@wf.job()
def finalize(
    paths: Annotated[list[str], Result(step="convert")],
    run_dir: RunDir = RunDir(),
) -> str:
    return "done"

if __name__ == "__main__":
    raise SystemExit(wf.cli())
```

```bash
python demo_flow.py dag
python demo_flow.py submit --help

export REFLOW_MODE=dry-run
python demo_flow.py submit \
    --run-dir /scratch/$USER/demo \
    --start 2025-01-01 --end 2025-01-31 \
    --bucket my-bucket --model icon
```

## Quick start (notebook)

```python
from typing import Annotated
from reflow import Workflow, Param, Result, RunDir

wf = Workflow("experiment")

@wf.job()
def prepare(start: Annotated[str, Param(help="Start")], run_dir: RunDir = RunDir()) -> list[str]:
    return ["a.nc", "b.nc"]

@wf.array_job()
def convert(
    nc_file: Annotated[str, Result(step="prepare")],
    bucket: Annotated[str, Param(help="Bucket")],
    run_dir: RunDir = RunDir(),
) -> str:
    return f"{nc_file}.zarr"

run = wf.submit(run_dir="/scratch/run1", start="2025-01-01", bucket="b")

run.status()              # pretty-prints task states
run.cancel()              # cancel all active tasks
run.cancel("convert")     # cancel one task
run.retry()               # retry all failures
run.status(as_dict=True)  # returns dict for scripting
```

## Installation

```bash
pip install -e .
pip install -e .[dev]   # pytest, ruff, mypy
```

## Core concepts

### `Result` -- data wiring

```python
@wf.array_job()
def convert(
    nc_file: Annotated[str, Result(step="prepare")],  # dependency + data
    run_dir: RunDir = RunDir(),
) -> str: ...
```

Multi-source concatenation:

```python
@wf.array_job()
def merge(
    item: Annotated[str, Result(steps=["prepare_a", "prepare_b"])],
) -> str: ...
```

### `RunDir` -- working directory

Injected as `pathlib.Path`.  Never appears on the CLI.

### `Param` -- CLI arguments

```python
start: Annotated[str, Param(help="Start date")]              # global: --start
chunk: Annotated[int, Param(help="Chunk", namespace="local")] = 256  # --ingest-chunk
model: Annotated[Literal["era5", "icon"], Param(help="Model")] = "era5"  # --model {era5,icon}
```

### `after` -- ordering without data

```python
@wf.job(after=["cleanup_old_runs"])
def prepare(run_dir: RunDir = RunDir()) -> list[str]: ...
```

## Reusable flows

```python
from reflow import Flow, Workflow

conversion = Flow("conversion")

@conversion.job()
def prepare(...) -> list[str]: ...

@conversion.array_job()
def convert(...) -> str: ...

# Single use
wf = Workflow("experiment")
wf.include(conversion)

# Or with prefixes
combined = Workflow("combined")
combined.include(conversion, prefix="era5")
combined.include(conversion, prefix="icon")

@combined.job()
def merge(
    era5: Annotated[list[str], Result(step="era5_convert")],
    icon: Annotated[list[str], Result(step="icon_convert")],
    run_dir: RunDir = RunDir(),
) -> str:
    return "merged"
```

## Data flow patterns

| Upstream returns | Upstream kind | Downstream wants | Downstream kind | Result |
|-----------------|---------------|-----------------|-----------------|--------|
| `list[str]`     | `job`         | `str`           | `array_job`     | fan-out |
| `str`           | `array_job`   | `list[str]`     | `job`           | gather |
| `str`           | `array_job`   | `str`           | `array_job`     | 1:1 chain |
| `list[str]`     | `array_job`   | `str`           | `array_job`     | flatten + fan-out |
| `list[str]`     | `array_job`   | `list[str]`     | `job`           | gather + flatten |
| `str`           | `job`         | `str`           | `job`           | direct |

## Merkle-DAG caching

Reflow uses a Merkle DAG to avoid redundant work.  Each task instance
gets an identity hash computed from:

- task name + version string
- direct input parameters (JSON-serialised)
- output hashes of all upstream dependencies

If a previous successful instance with the same identity exists in the
store, its output is reused without submitting a Slurm job.  This works
across runs -- resubmitting the same workflow with the same parameters
skips everything that already succeeded.

### How it works

```
prepare(start="2025-01-01", v=1)
    identity = hash(name + version + inputs)
    output_hash = hash(output)
         |
         v
convert[0](nc_file="a.nc", v=1)
    identity = hash(name + version + inputs + prepare.output_hash)
```

Change anything upstream and the hashes cascade forward -- every
downstream task gets a new identity and misses the cache.

### Controlling caching

```python
# Bump version when task logic changes (invalidates cache)
@wf.job(version="2")
def prepare(...) -> list[str]: ...

# Disable caching entirely for a task
@wf.job(cache=False)
def always_fresh(...) -> str: ...

# Force-run everything on submit
run = wf.submit(run_dir="...", force=True, start="2025-01-01")

# Force-run specific tasks only
run = wf.submit(run_dir="...", force_tasks=["prepare"], start="2025-01-01")
```

### Output verification (lazy by default)

By default, the Merkle identity is trusted on submit -- no filesystem
checks.  This is intentional: intermediate files may have been cleaned
up (and should be), and checking every cached output would penalise
good housekeeping.

Verification runs automatically **on retry**.  When something fails,
`run.retry()` walks the upstream chain, checks that cached outputs
are still valid (file existence for `Path` types, custom callables),
and invalidates anything stale:

```python
# Normal submit: trusts the cache, fast
run = wf.submit(run_dir="...", start="2025-01-01")

# Task fails -> retry verifies upstream before resubmitting
run.retry()                          # verify=True by default
run.retry("convert")                 # verify one task's upstreams
run.retry("convert", verify=False)   # skip verification if you're sure
```

For critical pipelines, opt in to proactive verification on submit:

```python
run = wf.submit(run_dir="...", verify=True, start="2025-01-01")
```

Tasks returning `Path` or `list[Path]` are verified by checking file
existence.  For custom logic, pass a callable:

```python
@wf.job(verify=lambda output: output > 0)
def compute_stats(...) -> float: ...

@wf.job()
def download(...) -> list[Path]:  # auto-verified via Path type
    return [Path("/scratch/era5.nc")]
```

### Per-instance caching for array jobs

Each array instance is cached individually.  If 98 out of 100 instances
are cached but 2 are stale, only those 2 are submitted to Slurm.

## Config file

`~/.config/reflow/config.toml`:

```toml
[executor]
partition = "compute"
account = "bm1159"
python = "/sw/spack-levante/mambaforge-23.1/bin/python"
mode = "sbatch"

[defaults]
run_dir = "/scratch/k204221/reflow"

[server]
url = "https://flow.dkrz.de"
```

Falls back to `REFLOW_*` environment variables.

## CLI commands

```bash
python flow.py submit --run-dir ... --start ... --bucket ...
python flow.py status <run-id> --run-dir ...
python flow.py cancel <run-id> --run-dir ...
python flow.py retry <run-id> --run-dir ...
python flow.py runs --run-dir ...
python flow.py dag
python flow.py describe   # JSON manifest for future server registration
```

## Workflow registration (planned)

```bash
reflow register demo_flow.py --server https://flow.dkrz.de
reflow list --server https://flow.dkrz.de
reflow inspect demo --server https://flow.dkrz.de
reflow delete demo --server https://flow.dkrz.de
```

The `describe` command already produces the JSON manifest that a server
would need to reconstruct the CLI and dispatch logic without importing
user code.

## Package structure

```
reflow/
├── __init__.py          # public exports, __version__
├── _types.py            # TaskState, RunState enums
├── params.py            # Param, Result, RunDir, type validation
├── cache.py             # Merkle identity hashing and verification
├── flow.py              # Flow (reusable task registry)
├── workflow.py          # Workflow (extends Flow with execution)
├── run.py               # Run handle for interactive use
├── cli.py               # argparse CLI
├── config.py            # config file + env var loading
├── stores/
│   ├── __init__.py      # Store ABC
│   └── sqlite.py        # SqliteStore (stdlib sqlite3)
└── executors/
    ├── __init__.py      # Executor ABC, JobResources
    ├── slurm.py         # SlurmExecutor
    └── local.py         # LocalExecutor
```

## Design choices

**Why zero heavy dependencies?**  HPC environments have constrained
package availability.  stdlib sqlite3 + argparse means this works
everywhere Python 3.10+ is installed.

**Why `Result` annotations?**  Dependencies and data flow declared in
one place -- the function signature.  The framework infers fan-out,
gather, and chaining from the type relationship.

**Why `Flow` and `Workflow`?**  Separation of reusable task definitions
from concrete execution machinery.  Define once, include in multiple
workflows with optional prefixes.

**Why `Store` ABC?**  SQLite for local single-user runs, future REST
server for multi-user production.  The orchestration core is
storage-agnostic.

**Why `Run` handles?**  Notebooks need an object that remembers
connection details.  `run.status()` beats passing five arguments.

**Why static type validation?**  Catch wiring errors before submitting
to Slurm, not 30 minutes into a batch job.

**Why Merkle-DAG caching?**  Change anything upstream and the hashes
cascade forward automatically -- no manual invalidation.  Cross-run
caching means resubmitting the same workflow skips completed work.
Tasks returning ``Path`` are auto-verified by checking file existence.
