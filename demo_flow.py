#!/usr/bin/env python
"""Demo workflow for CLI usage.

Run::

    python demo_flow.py dag
    python demo_flow.py submit --help

    export REFLOW_MODE=dry-run
    python demo_flow.py submit \\
        --run-dir /tmp/demo \\
        --start 2025-01-01 \\
        --end 2025-01-31 \\
        --bucket my-bucket \\
        --s3-secret /path/to/creds.json
"""

from __future__ import annotations

from typing import Annotated, Literal

from reflow import Param, Result, RunDir, Workflow

wf = Workflow("demo")


@wf.job()
def prepare(
    start: Annotated[str, Param(help="Start date, ISO-8601")],
    end: Annotated[str, Param(help="End date, ISO-8601")],
    s3_secret: Annotated[str, Param(help="Path to S3 credential file")],
    model: Annotated[Literal["era5", "icon", "cmip6"], Param(help="Model")] = "era5",
    run_dir: RunDir = RunDir(),
) -> list[str]:
    """Discover input files for the date range."""
    print(f"prepare: {start} -> {end} (model={model})")
    return ["a.nc", "b.nc", "c.nc"]


@wf.array_job(cpus=4, time="00:10:00")
def convert(
    nc_file: Annotated[str, Result(step="prepare")],
    bucket: Annotated[str, Param(help="Target S3 bucket")],
    run_dir: RunDir = RunDir(),
) -> str:
    """Convert one netCDF to zarr."""
    print(f"convert: {nc_file} -> s3://{bucket}/")
    return f"/data/{nc_file}.zarr"


@wf.array_job()
def validate(
    zarr_path: Annotated[str, Result(step="convert")],
    run_dir: RunDir = RunDir(),
) -> str:
    """Validate one zarr file (1:1 chain)."""
    print(f"validate: {zarr_path}")
    return f"ok:{zarr_path}"


@wf.job()
def finalize(
    validated: Annotated[list[str], Result(step="validate")],
    bucket: Annotated[str, Param(help="Target S3 bucket")],
    run_dir: RunDir = RunDir(),
) -> str:
    """Write completion marker."""
    print(f"finalize: {len(validated)} files, bucket={bucket}")
    return "done"


if __name__ == "__main__":
    raise SystemExit(wf.cli())
