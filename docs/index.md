# Reflow

Reflow is a decorator-based Python DAG library for HPC-style workflows.

The core idea is simple:

- define tasks as normal Python functions
- register them with `@wf.job()` or `@wf.array_job()`
- let Reflow infer ordering and data dependencies
- submit the graph through an executor such as Slurm
- persist run state in a shared SQLite manifest store

This documentation is intentionally **example-first**. The fastest way to understand Reflow is to read a complete workflow and then zoom in on the pieces.

## A complete example

```python
from pathlib import Path
from typing import Annotated

from reflow import Param, Result, RunDir, Workflow

wf = Workflow("zarr-pipeline")


@wf.job(time="00:10:00", mem="2G")
def prepare(
    source: Annotated[str, Param(help="Input NetCDF file")],
    run_dir: RunDir = RunDir(),
) -> list[str]:
    inputs = [source]
    (Path(run_dir) / "inputs.txt").write_text("\n".join(inputs))
    return inputs


@wf.array_job(time="00:30:00", mem="8G")
def convert(
    item: Annotated[str, Result(step="prepare")],
    bucket: Annotated[str, Param(help="Destination bucket")],
) -> str:
    return f"{bucket}/{Path(item).stem}.zarr"


@wf.job(time="00:05:00", mem="1G")
def publish(
    paths: Annotated[list[str], Result(step="convert")],
    run_dir: RunDir = RunDir(),
) -> Path:
    out = Path(run_dir) / "published.txt"
    out.write_text("\n".join(paths))
    return out


if __name__ == "__main__":
    wf.cli()
```

Run it with:

```bash
python flow.py submit \
  --run-dir /scratch/my-run \
  --source /data/input.nc \
  --bucket climate-demo
```

## What to read next

- Start with the [tutorial](tutorial.md)
- Then read [Parameters and CLI](parameters-and-cli.md)
- Then read [Dependencies and data wiring](dependencies-and-wiring.md)
- For persistence and caching, read [Shared manifest store](shared-manifest-store.md)

## Design goals

- **No heavy runtime dependencies** in the library itself
- **Typed task wiring** based on Python annotations
- **Shared manifest store** so runs and cache entries work across multiple run directories
- **Executor abstraction** so Slurm is only the first backend, not the last
