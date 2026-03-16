#!/usr/bin/env python
"""Demo DAG showing the Annotated + Param API.

This demonstrates how parameters are declared Typer-style using
``Annotated[type, Param(...)]``.  Global params are shared across tasks;
local params are prefixed with the task name on the CLI.

Run::

    python demo_flow.py --help
    python demo_flow.py submit --help
    python demo_flow.py dag
"""

from __future__ import annotations

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
    """Discover input files for the date range."""
    print(f"prepare: {start} -> {end}")
    print(f"  s3_secret = {s3_secret}")
    return {"files": ["a.nc", "b.nc", "c.nc"]}


@graph.array_job(expand="prepare.files", cpus=4, time="00:10:00")
def convert(
    item: str,
    bucket: Annotated[str, Param(help="Target S3 bucket")],
    chunk_size: Annotated[int, Param(
        help="Chunk size in MB (convert-specific)",
        namespace="local",
    )] = 256,
    run_dir: str = "",
) -> Dict[str, str]:
    """Convert one file and upload to the bucket."""
    print(f"convert: {item} -> s3://{bucket}/ (chunk={chunk_size})")
    return {"item": item, "status": "uploaded"}


@graph.job(after=["convert"])
def finalize(
    bucket: Annotated[str, Param(help="Target S3 bucket")],
    notification_email: Annotated[str, Param(
        short="-n",
        help="Email for completion notification",
    )] = "",
    run_dir: str = "",
) -> Dict[str, str]:
    """Write a completion marker after all conversions finish."""
    print(f"finalize: bucket={bucket}")
    if notification_email:
        print(f"  would notify {notification_email}")
    return {"status": "ok"}


if __name__ == "__main__":
    raise SystemExit(graph.cli())
