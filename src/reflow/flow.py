"""Reusable task registry for reflow.

A :class:`Flow` holds task definitions (registered via decorators) but
has no execution machinery.  Attach a flow to a
:class:`~reflow.Workflow` with :meth:`Workflow.include`.
"""

from __future__ import annotations

import copy
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable

from .params import Result, collect_result_deps, get_return_type


@dataclass
class JobConfig:
    """Resource and dependency configuration for a single task."""

    cpus: int = 1
    time: str = "00:30:00"
    mem: str = "4G"
    partition: str = "compute"
    account: str | None = None
    array: bool = False
    after: list[str] = field(default_factory=list)
    array_parallelism: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    version: str = "1"
    cache: bool = True
    mail_user: str | None = None
    mail_type: str | None = None  # e.g. "FAIL", "END", "FAIL,END", "ALL"
    signal: str | None = None     # e.g. "B:INT@60" (send SIGINT 60s before timeout)
    # verify is stored on TaskSpec, not here (not serialisable).


@dataclass
class TaskSpec:
    """Internal definition of one task."""

    name: str
    func: Callable[..., Any]
    config: JobConfig
    signature: inspect.Signature
    result_deps: dict[str, Result] = field(default_factory=dict)
    return_type: Any = None
    verify: Callable[[Any], bool] | None = None


class Flow:
    """Reusable collection of task definitions.

    Register tasks with :meth:`job` and :meth:`array_job`.  A flow has
    no execution machinery -- attach it to a :class:`~reflow.Workflow`
    via :meth:`Workflow.include`.

    Parameters
    ----------
    name : str
        Flow name.
    """

    def __init__(self, name: str) -> None:
        self.name: str = name
        self.tasks: dict[str, TaskSpec] = {}
        self._registration_order: list[str] = []

    def job(
        self,
        name: str | None = None,
        *,
        cpus: int = 1,
        time: str = "00:30:00",
        mem: str = "4G",
        partition: str = "compute",
        account: str | None = None,
        after: list[str] | None = None,
        extra: dict[str, Any] | None = None,
        version: str = "1",
        cache: bool = True,
        verify: Callable[[Any], bool] | None = None,
        mail_user: str | None = None,
        mail_type: str | None = None,
        signal: str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a singleton task.

        Parameters
        ----------
        name : str or None
            Task name.  Defaults to the function name.
        cpus : int
            CPUs per task.
        time : str
            Wall-clock time limit.
        mem : str
            Memory request.
        partition : str
            Target partition.
        account : str or None
            Billing account.
        after : list[str] or None
            Explicit ordering dependencies.
        extra : dict[str, Any] or None
            Backend-specific resource flags.
        version : str
            Cache version string.  Bump to invalidate cached results
            when the task logic changes.
        cache : bool
            Whether to cache results across runs.
        verify : callable or None
            Custom output verification function.
        mail_user : str or None
            Email address for Slurm notifications.  Falls back to
            the config file / ``REFLOW_MAIL_USER`` if not set.
        mail_type : str or None
            Notification type (e.g. ``"FAIL"``, ``"END"``, ``"ALL"``).
            Falls back to config / ``REFLOW_MAIL_TYPE``.
        signal : str or None
            Signal spec sent before timeout, e.g. ``"B:INT@60"``
            (send SIGINT 60 seconds before walltime).  Falls back to
            config / ``REFLOW_SIGNAL``.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or func.__name__
            self._register(
                task_name, func,
                JobConfig(
                    cpus=cpus, time=time, mem=mem, partition=partition,
                    account=account, after=list(after or []),
                    array=False, extra=dict(extra or {}),
                    version=version, cache=cache,
                    mail_user=mail_user, mail_type=mail_type,
                    signal=signal,
                ),
                verify=verify,
            )
            return func

        return decorator

    def array_job(
        self,
        name: str | None = None,
        *,
        cpus: int = 1,
        time: str = "00:30:00",
        mem: str = "4G",
        partition: str = "compute",
        account: str | None = None,
        after: list[str] | None = None,
        array_parallelism: int | None = None,
        extra: dict[str, Any] | None = None,
        version: str = "1",
        cache: bool = True,
        verify: Callable[[Any], bool] | None = None,
        mail_user: str | None = None,
        mail_type: str | None = None,
        signal: str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register an array task.

        Parameters
        ----------
        name : str or None
            Task name.  Defaults to the function name.
        cpus : int
            CPUs per task.
        time : str
            Wall-clock time limit.
        mem : str
            Memory request.
        partition : str
            Target partition.
        account : str or None
            Billing account.
        after : list[str] or None
            Additional explicit ordering dependencies.
        array_parallelism : int or None
            Maximum concurrent array tasks.
        extra : dict[str, Any] or None
            Backend-specific resource flags.
        version : str
            Cache version string.
        cache : bool
            Whether to cache results across runs.
        verify : callable or None
            Custom output verification function.
        mail_user : str or None
            Email address for Slurm notifications.
        mail_type : str or None
            Notification type.
        signal : str or None
            Signal spec sent before timeout.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or func.__name__
            self._register(
                task_name, func,
                JobConfig(
                    cpus=cpus, time=time, mem=mem, partition=partition,
                    account=account, after=list(after or []),
                    array=True, array_parallelism=array_parallelism,
                    extra=dict(extra or {}),
                    version=version, cache=cache,
                    mail_user=mail_user, mail_type=mail_type,
                    signal=signal,
                ),
                verify=verify,
            )
            return func

        return decorator

    def _register(
        self,
        task_name: str,
        func: Callable[..., Any],
        config: JobConfig,
        verify: Callable[[Any], bool] | None = None,
    ) -> None:
        if task_name in self.tasks:
            raise ValueError(f"Task {task_name!r} is already registered.")
        result_deps = collect_result_deps(func)
        if config.array and not result_deps:
            raise ValueError(
                f"Array job {task_name!r} must have at least one parameter "
                f"annotated with Result(step=...)."
            )
        self.tasks[task_name] = TaskSpec(
            name=task_name, func=func, config=config,
            signature=inspect.signature(func),
            result_deps=result_deps, return_type=get_return_type(func),
            verify=verify,
        )
        self._registration_order.append(task_name)

    def _prefixed_tasks(
        self, prefix: str | None,
    ) -> tuple[dict[str, TaskSpec], list[str]]:
        """Return deep copies of tasks with prefixed names and references."""
        if prefix is None:
            return (
                {n: copy.deepcopy(s) for n, s in self.tasks.items()},
                list(self._registration_order),
            )

        def _px(name: str) -> str:
            return f"{prefix}_{name}"

        internal = set(self.tasks.keys())
        tasks: dict[str, TaskSpec] = {}
        order: list[str] = []

        for old_name, old_spec in self.tasks.items():
            new_name = _px(old_name)
            new_config = copy.deepcopy(old_spec.config)
            new_config.after = [
                _px(a) if a in internal else a for a in new_config.after
            ]
            new_deps: dict[str, Result] = {}
            for pname, result in old_spec.result_deps.items():
                new_deps[pname] = Result(
                    steps=[_px(s) if s in internal else s for s in result.steps],
                )
            tasks[new_name] = TaskSpec(
                name=new_name, func=old_spec.func, config=new_config,
                signature=old_spec.signature,
                result_deps=new_deps, return_type=old_spec.return_type,
            )
            order.append(new_name)

        return tasks, order
