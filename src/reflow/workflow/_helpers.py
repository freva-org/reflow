"""Module-level helper functions for the workflow package."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, get_type_hints

from .._types import TaskState  # noqa: F401 — re-exported for _worker
from ..executors.helpers import default_executor, resolve_executor
from ..flow import TaskSpec
from ..params import is_run_dir

__all__ = ["build_kwargs", "default_executor", "make_run_id", "resolve_executor"]


def make_run_id(workflow_name: str) -> str:
    """Generate a short, human-friendly run id.

    Format: ``<workflow>-<YYYYMMDD>-<4hex>``
    """
    from datetime import datetime, timezone

    date_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
    short = uuid.uuid4().hex[:4]
    return f"{workflow_name}-{date_str}-{short}"


def resolve_index(explicit: int | None) -> int | None:
    """Return the array index from *explicit* or a scheduler env var.

    Checks environment variables from all supported schedulers in a
    fixed priority order.  The first one found wins.
    """
    if explicit is not None:
        return explicit
    _ARRAY_INDEX_VARS = (
        "SLURM_ARRAY_TASK_ID",  # Slurm
        "PBS_ARRAY_INDEX",  # PBS Pro / OpenPBS
        "PBS_ARRAYID",  # Torque
        "LSB_JOBINDEX",  # LSF
        "SGE_TASK_ID",  # SGE / UGE
        "FLUX_JOB_CC",  # Flux
        "OAR_ARRAY_INDEX",  # OAR
    )
    for var in _ARRAY_INDEX_VARS:
        env_val = os.getenv(var)
        if env_val is not None:
            try:
                return int(env_val)
            except ValueError:
                continue
    return None


def build_kwargs(
    spec: TaskSpec,
    run_parameters: dict[str, Any],
    task_input: dict[str, Any],
) -> dict[str, Any]:
    """Build function kwargs for a worker invocation.

    Resolution order: RunDir -> Result (task_input) -> task-local -> global.
    """
    try:
        hints = get_type_hints(spec.func, include_extras=True)
    except Exception:
        hints = {}

    task_locals: dict[str, Any] = run_parameters.get("__task_params__", {}).get(
        spec.name, {}
    )

    kwargs: dict[str, Any] = {}
    for pname in spec.signature.parameters:
        ann = hints.get(pname)
        if (ann is not None and is_run_dir(ann)) or pname == "run_dir":
            kwargs[pname] = Path(run_parameters.get("run_dir", "."))
            continue
        if pname in spec.result_deps and pname in task_input:
            kwargs[pname] = task_input[pname]
            continue
        if pname in task_locals:
            kwargs[pname] = task_locals[pname]
            continue
        if pname in run_parameters:
            kwargs[pname] = run_parameters[pname]

    return kwargs
