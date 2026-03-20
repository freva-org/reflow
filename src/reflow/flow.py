"""Reusable task registry for reflow.

A [`Flow`][Flow] holds task definitions (registered via decorators) but
has no execution machinery. Attach a flow to a
[`reflow.Workflow`][reflow.Workflow] with [`Workflow.include`][Workflow.include].
"""

from __future__ import annotations

import copy
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .params import Result, collect_result_deps, get_return_type


@dataclass(init=False)
class JobConfig:
    """Resource and dependency configuration for a single task."""

    cpus: int
    time: str
    mem: str
    array: bool
    after: list[str]
    array_parallelism: int | None
    submit_options: dict[str, Any]
    version: str
    cache: bool
    backend: str | None

    def __init__(
        self,
        cpus: int = 1,
        time: str = "00:30:00",
        mem: str = "4G",
        array: bool = False,
        after: list[str] | None = None,
        array_parallelism: int | None = None,
        submit_options: dict[str, Any] | None = None,
        version: str = "1",
        cache: bool = True,
        backend: str | None = None,
        extra: dict[str, Any] | None = None,
        **scheduler_options: Any,
    ) -> None:
        merged = dict(submit_options or {})
        if extra:
            merged.update(extra)
        merged.update(scheduler_options)

        self.cpus = cpus
        self.time = time
        self.mem = mem
        self.array = array
        self.after = list(after or [])
        self.array_parallelism = array_parallelism
        self.submit_options = merged
        self.version = version
        self.cache = cache
        self.backend = backend

    @property
    def extra(self) -> dict[str, Any]:
        """Backward-compatible alias for scheduler-native submit options."""
        return self.submit_options

    @property
    def partition(self) -> str | None:
        return self.submit_options.get("partition")

    @property
    def account(self) -> str | None:
        return self.submit_options.get("account")

    @property
    def mail_user(self) -> str | None:
        return self.submit_options.get("mail_user")

    @property
    def mail_type(self) -> str | None:
        return self.submit_options.get("mail_type")

    @property
    def signal(self) -> str | None:
        return self.submit_options.get("signal")


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

    Register tasks with [`job`][job] and [`array_job`][array_job]. A flow has
    no execution machinery -- attach it to a [`reflow.Workflow`][reflow.Workflow]
    via [`Workflow.include`][Workflow.include].

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
        after: list[str] | None = None,
        submit_options: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
        version: str = "1",
        cache: bool = True,
        verify: Callable[[Any], bool] | None = None,
        backend: str | None = None,
        **scheduler_options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a singleton task.

        Parameters
        ----------
        name : str or None
            Task name. Defaults to the function name.
        cpus : int
            CPUs per task.
        time : str
            Wall-clock time limit.
        mem : str
            Memory request.
        after : list[str] or None
            Explicit ordering dependencies.
        submit_options : dict[str, Any] or None
            Scheduler-native submit options, for example ``partition``,
            ``account``, ``qos``, or ``queue``.
        extra : dict[str, Any] or None
            Backward-compatible alias for ``submit_options``.
        version : str
            Cache version string. Bump to invalidate cached results
            when the task logic changes.
        cache : bool
            Whether to cache results across runs.
        verify : callable or None
            Custom output verification function.
        backend : str or None
            Optional backend hint for future multi-backend execution.
        **scheduler_options : Any
            Additional scheduler-native submit options. These are merged
            with ``submit_options`` and stored verbatim.

        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or func.__name__
            self._register(
                task_name,
                func,
                JobConfig(
                    cpus=cpus,
                    time=time,
                    mem=mem,
                    after=list(after or []),
                    array=False,
                    submit_options=submit_options,
                    extra=extra,
                    version=version,
                    cache=cache,
                    backend=backend,
                    **scheduler_options,
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
        after: list[str] | None = None,
        array_parallelism: int | None = None,
        submit_options: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
        version: str = "1",
        cache: bool = True,
        verify: Callable[[Any], bool] | None = None,
        backend: str | None = None,
        **scheduler_options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register an array task.

        Parameters
        ----------
        name : str or None
            Task name. Defaults to the function name.
        cpus : int
            CPUs per task.
        time : str
            Wall-clock time limit.
        mem : str
            Memory request.
        after : list[str] or None
            Additional explicit ordering dependencies.
        array_parallelism : int or None
            Maximum concurrent array tasks.
        submit_options : dict[str, Any] or None
            Scheduler-native submit options, for example ``partition``,
            ``account``, ``qos``, or ``queue``.
        extra : dict[str, Any] or None
            Backward-compatible alias for ``submit_options``.
        version : str
            Cache version string.
        cache : bool
            Whether to cache results across runs.
        verify : callable or None
            Custom output verification function.
        backend : str or None
            Optional backend hint for future multi-backend execution.
        **scheduler_options : Any
            Additional scheduler-native submit options. These are merged
            with ``submit_options`` and stored verbatim.

        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or func.__name__
            self._register(
                task_name,
                func,
                JobConfig(
                    cpus=cpus,
                    time=time,
                    mem=mem,
                    after=list(after or []),
                    array=True,
                    array_parallelism=array_parallelism,
                    submit_options=submit_options,
                    extra=extra,
                    version=version,
                    cache=cache,
                    backend=backend,
                    **scheduler_options,
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
            name=task_name,
            func=func,
            config=config,
            signature=inspect.signature(func),
            result_deps=result_deps,
            return_type=get_return_type(func),
            verify=verify,
        )
        self._registration_order.append(task_name)

    def _prefixed_tasks(
        self,
        prefix: str | None,
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
            spec = copy.deepcopy(old_spec)
            spec.name = new_name
            spec.config.after = [
                _px(d) if d in internal else d for d in spec.config.after
            ]
            new_result_deps: dict[str, Result] = {}
            for pname, res in spec.result_deps.items():
                steps = [_px(s) if s in internal else s for s in res.steps]
                new_result_deps[pname] = Result(steps=steps)
            spec.result_deps = new_result_deps
            tasks[new_name] = spec
            order.append(new_name)

        return tasks, order
