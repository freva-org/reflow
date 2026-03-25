"""Module-level helper functions for the workflow package."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, get_type_hints

from .._types import TaskState  # noqa: F401 — re-exported for _worker
from ..flow import TaskSpec
from ..params import is_run_dir


def _make_run_id(workflow_name: str) -> str:
    """Generate a short, human-friendly run id.

    Format: ``<workflow>-<YYYYMMDD>-<4hex>``
    """
    from datetime import datetime, timezone

    date_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
    short = uuid.uuid4().hex[:4]
    return f"{workflow_name}-{date_str}-{short}"


def _resolve_executor(executor: Any) -> Any:
    """Resolve an executor shorthand string to an instance.

    Parameters
    ----------
    executor : Executor, str, or None
        ``"local"`` is a shorthand for
        :class:`~reflow.executors.local.LocalExecutor`.

    """
    if isinstance(executor, str):
        if executor == "local":
            from ..executors.local import LocalExecutor

            return LocalExecutor()
        raise ValueError(f"Unknown executor shorthand {executor!r}.  Use 'local'.")
    return executor


def _resolve_index(explicit: int | None) -> int | None:
    """Return the array index from *explicit* or ``SLURM_ARRAY_TASK_ID``."""
    if explicit is not None:
        return explicit
    env_val = os.getenv("SLURM_ARRAY_TASK_ID")
    return int(env_val) if env_val is not None else None


def _build_kwargs(
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
