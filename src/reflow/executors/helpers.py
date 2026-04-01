"""Helpers for resolving the workload manager executor."""

from __future__ import annotations

import os
from typing import Any

from .flux import FluxExecutor
from .local import LocalExecutor
from .lsf import LSFExecutor
from .pbs import PBSExecutor
from .sge import SGEExecutor
from .slurm import SlurmExecutor


def default_executor(config: Any) -> Any:
    """Return the default executor based on config mode.

    Reads ``config.executor_mode`` (or the ``REFLOW_MODE`` env var)
    to decide which backend to instantiate.  Falls back to
    [`reflow.executors.slurm.SlurmExecutor`][reflow.executors.slurm.SlurmExecutor]
    when no mode is configured.

    Parameters
    ----------
    config : Config
        User configuration.

    Returns
    -------
    Executor
        An executor instance for the configured backend.

    """
    mode = (
        (os.getenv("REFLOW_MODE") or getattr(config, "executor_mode", None) or "")
        .strip()
        .lower()
    )

    if mode in ("qsub-pbs", "qsub"):
        return PBSExecutor.from_environment(config)

    if mode == "bsub":
        return LSFExecutor.from_environment(config)

    if mode == "qsub-sge":
        return SGEExecutor.from_environment(config)

    if mode == "flux":
        return FluxExecutor.from_environment(config)

    # Default: Slurm (covers "sbatch", "dry-run", and empty/unset).
    return SlurmExecutor.from_environment(config)


def resolve_executor(executor: Any) -> Any:
    """Resolve an executor shorthand string to an instance.

    Parameters
    ----------
    executor : Executor, str, or None
        Supported shorthands: ``"local"``, ``"pbs"``, ``"lsf"``,
        ``"sge"``, ``"flux"``.

    """
    if isinstance(executor, str):
        name = executor.strip().lower()
        if name == "local":
            return LocalExecutor()
        if name == "pbs":
            return PBSExecutor.from_environment()
        if name == "lsf":
            return LSFExecutor.from_environment()
        if name == "sge":
            return SGEExecutor.from_environment()
        if name == "flux":
            return FluxExecutor.from_environment()
        raise ValueError(
            f"Unknown executor shorthand {executor!r}.  "
            f"Use 'local', 'pbs', 'lsf', 'sge', or 'flux'."
        )
    return executor
