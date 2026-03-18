"""Notebook-style usage of reflow.

Demonstrates reusable flows, prefixed includes, and the interactive
``Run`` handle.

Usage::

    %run demo_notebook.py
"""

from __future__ import annotations

import os
import tempfile
from typing import Annotated

from reflow import Flow, Param, Result, RunDir, Workflow

# --- reusable flow ---------------------------------------------------------

conversion = Flow("conversion")


@conversion.job()
def prepare(
    start: Annotated[str, Param(help="Start date")],
    end: Annotated[str, Param(help="End date")],
    run_dir: RunDir = RunDir(),
) -> list[str]:
    print(f"prepare: {start} -> {end}")
    return ["a.nc", "b.nc", "c.nc"]


@conversion.array_job()
def convert(
    nc_file: Annotated[str, Result(step="prepare")],
    bucket: Annotated[str, Param(help="S3 bucket")],
    run_dir: RunDir = RunDir(),
) -> str:
    print(f"convert: {nc_file} -> s3://{bucket}/")
    return f"/data/{nc_file}.zarr"


@conversion.job()
def finalize(
    paths: Annotated[list[str], Result(step="convert")],
    run_dir: RunDir = RunDir(),
) -> str:
    print(f"finalize: {len(paths)} files")
    return "done"


# --- scenario 1: single workflow -------------------------------------------

if __name__ == "__main__":
    os.environ["REFLOW_MODE"] = "dry-run"

    wf = Workflow("experiment_1")
    wf.include(conversion)

    print("=== DAG ===")
    for name in wf._topological_order():
        spec = wf.tasks[name]
        deps = wf._effective_dependencies(spec)
        tag = " [array]" if spec.config.array else ""
        dep_str = f"  <- {', '.join(deps)}" if deps else ""
        print(f"  {name}{tag}{dep_str}")

    print()
    print("=== Submit (dry-run) ===")
    with tempfile.TemporaryDirectory() as tmp:
        run = wf.submit(
            run_dir=tmp, start="2025-01-01", end="2025-01-31",
            bucket="my-bucket",
        )
        print(f"run = {run}")
        print()
        print("=== Status ===")
        run.status()

    # --- scenario 2: two flows with prefixes ------------------------------

    print()
    print("=== Combined workflow with prefixes ===")

    combined = Workflow("combined")
    combined.include(conversion, prefix="era5")
    combined.include(conversion, prefix="icon")

    @combined.job()
    def merge(
        era5: Annotated[list[str], Result(step="era5_convert")],
        icon: Annotated[list[str], Result(step="icon_convert")],
        run_dir: RunDir = RunDir(),
    ) -> str:
        print(f"merge: {len(era5)} + {len(icon)} files")
        return "merged"

    for name in combined._topological_order():
        spec = combined.tasks[name]
        deps = combined._effective_dependencies(spec)
        tag = " [array]" if spec.config.array else ""
        dep_str = f"  <- {', '.join(deps)}" if deps else ""
        print(f"  {name}{tag}{dep_str}")

    print()
    print("=== Describe (JSON manifest) ===")
    import json
    manifest = combined.describe()
    print(json.dumps(manifest, indent=2, default=str)[:500] + "\n  ...")
