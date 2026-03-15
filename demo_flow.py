#!/usr/bin/env python
"""Demo DAG that illustrates slurm_flow's decorator API.

Run ``python demo_flow.py --help`` to see the generated CLI.
"""

from __future__ import annotations

from typing import Dict, List

from slurm_flow import Graph

graph = Graph("demo")


@graph.job(cpus=2, time="00:05:00")
def prepare(start: str, end: str, run_dir: str) -> Dict[str, List[str]]:
    """Discover input files for the date range."""
    print(f"prepare: {start} → {end}")
    return {"files": ["a.nc", "b.nc", "c.nc"]}


@graph.array_job(expand="prepare.files", cpus=4, time="00:10:00")
def convert(item: str, bucket: str, run_dir: str) -> Dict[str, str]:
    """Convert one file and upload to the bucket."""
    print(f"convert: {item} → s3://{bucket}/")
    return {"item": item, "status": "uploaded"}


@graph.job(after=["convert"])
def finalize(bucket: str, run_dir: str) -> Dict[str, str]:
    """Write a completion marker after all conversions finish."""
    print(f"finalize: bucket={bucket}")
    return {"status": "ok"}


if __name__ == "__main__":
    raise SystemExit(graph.cli())
