"""Core DAG graph with decorator-based task registration.

This module provides the :class:`Graph` class which is the main user-facing
API for defining workflows.  Tasks are registered via :meth:`Graph.job` and
:meth:`Graph.array_job` decorators and the resulting DAG is submitted,
dispatched, and executed through the :meth:`Graph.cli` entry point.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import sys
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from . import db as _db
from ._types import RunState, TaskState
from .executors import Executor, JobResources
from .executors.slurm import SlurmExecutor

logger = logging.getLogger(__name__)


# ── dataclasses ────────────────────────────────────────────────────────


@dataclass
class JobConfig:
    """Resource and dependency configuration for a single task.

    Parameters
    ----------
    cpus : int
        CPUs per task.
    time : str
        Wall-clock time limit, e.g. ``"01:00:00"``.
    mem : str
        Memory request, e.g. ``"4G"``.
    partition : str
        Target partition or queue.
    account : str or None
        Billing account (``-A`` in Slurm).
    array : bool
        Whether this task is an array job.
    expand : str or None
        Expansion expression, e.g. ``"prepare.files"``.
    after : List[str]
        Explicit upstream dependency names.
    array_parallelism : int or None
        Maximum concurrent array tasks (e.g. ``%16``).
    extra : Dict[str, Any]
        Arbitrary backend-specific resource flags.
    """

    cpus: int = 1
    time: str = "00:30:00"
    mem: str = "4G"
    partition: str = "compute"
    account: Optional[str] = None
    array: bool = False
    expand: Optional[str] = None
    after: List[str] = field(default_factory=list)
    array_parallelism: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskSpec:
    """Internal definition of one graph task.

    Parameters
    ----------
    name : str
        Unique task name.
    func : Callable
        Decorated Python function.
    config : JobConfig
        Resource and dependency settings.
    signature : inspect.Signature
        Introspected function signature.
    """

    name: str
    func: Callable[..., Any]
    config: JobConfig
    signature: inspect.Signature


# ── the graph ──────────────────────────────────────────────────────────


class Graph:
    """Decorator-based Slurm DAG.

    Define tasks with :meth:`job` and :meth:`array_job`, then call
    :meth:`cli` from your script entry point.

    Parameters
    ----------
    name : str
        Graph name.  Used as the CLI program name and as a prefix
        for Slurm job names.

    Examples
    --------
    >>> from slurm_flow import Graph
    >>> graph = Graph("my_pipeline")
    >>>
    >>> @graph.job(cpus=2, time="00:05:00")
    ... def prepare(start: str, end: str, run_dir: str) -> Dict[str, List[str]]:
    ...     return {"files": ["a.nc", "b.nc"]}
    >>>
    >>> @graph.array_job(expand="prepare.files", cpus=4)
    ... def convert(item: str, bucket: str) -> Dict[str, str]:
    ...     return {"item": item}
    >>>
    >>> @graph.job(after=["convert"])
    ... def finalize(bucket: str) -> Dict[str, str]:
    ...     return {"status": "ok"}
    >>>
    >>> graph.cli()
    """

    def __init__(self, name: str) -> None:
        self.name: str = name
        self.tasks: Dict[str, TaskSpec] = {}
        self._registration_order: List[str] = []

    # ── decorators ────────────────────────────────────────────────────

    def job(
        self,
        name: Optional[str] = None,
        *,
        cpus: int = 1,
        time: str = "00:30:00",
        mem: str = "4G",
        partition: str = "compute",
        account: Optional[str] = None,
        after: Optional[List[str]] = None,
        extra: Optional[Dict[str, Any]] = None,
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
        after : List[str] or None
            Explicit upstream dependency names.
        extra : Dict[str, Any] or None
            Backend-specific resource flags.

        Returns
        -------
        Callable
            Decorator.
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
                    partition=partition,
                    account=account,
                    after=list(after or []),
                    array=False,
                    expand=None,
                    extra=dict(extra or {}),
                ),
            )
            return func

        return decorator

    def array_job(
        self,
        name: Optional[str] = None,
        *,
        expand: str,
        cpus: int = 1,
        time: str = "00:30:00",
        mem: str = "4G",
        partition: str = "compute",
        account: Optional[str] = None,
        after: Optional[List[str]] = None,
        array_parallelism: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register an array task that expands from upstream output.

        Parameters
        ----------
        name : str or None
            Task name.  Defaults to the function name.
        expand : str
            Expansion expression of the form ``"upstream_task.output_key"``.
            The upstream task is added as an implicit dependency.
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
        after : List[str] or None
            Additional explicit upstream dependencies.
        array_parallelism : int or None
            Maximum concurrent array tasks.
        extra : Dict[str, Any] or None
            Backend-specific resource flags.

        Returns
        -------
        Callable
            Decorator.
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
                    partition=partition,
                    account=account,
                    after=list(after or []),
                    array=True,
                    expand=expand,
                    array_parallelism=array_parallelism,
                    extra=dict(extra or {}),
                ),
            )
            return func

        return decorator

    def _register(
        self,
        task_name: str,
        func: Callable[..., Any],
        config: JobConfig,
    ) -> None:
        """Store a task spec after validation."""
        if task_name in self.tasks:
            raise ValueError(f"Task {task_name!r} is already registered.")
        self.tasks[task_name] = TaskSpec(
            name=task_name,
            func=func,
            signature=inspect.signature(func),
            config=config,
        )
        self._registration_order.append(task_name)

    # ── dependency resolution ─────────────────────────────────────────

    def _effective_dependencies(self, spec: TaskSpec) -> List[str]:
        """Return the full dependency list including implicit expand deps.

        Parameters
        ----------
        spec : TaskSpec
            Task specification.

        Returns
        -------
        List[str]
            Deduplicated, sorted dependency names.
        """
        deps: Set[str] = set(spec.config.after)
        if spec.config.expand:
            upstream = spec.config.expand.split(".", 1)[0]
            deps.add(upstream)
        return sorted(deps)

    def _topological_order(self) -> List[str]:
        """Return task names in topological order.

        Returns
        -------
        List[str]
            Task names ordered so that dependencies come first.

        Raises
        ------
        ValueError
            If the graph contains a cycle.
        """
        in_degree: Dict[str, int] = {name: 0 for name in self.tasks}
        children: Dict[str, List[str]] = {name: [] for name in self.tasks}

        for name, spec in self.tasks.items():
            for dep in self._effective_dependencies(spec):
                if dep not in self.tasks:
                    raise ValueError(
                        f"Task {name!r} depends on unknown task {dep!r}."
                    )
                in_degree[name] += 1
                children[dep].append(name)

        queue = [n for n in self._registration_order if in_degree[n] == 0]
        order: List[str] = []

        while queue:
            node = queue.pop(0)
            order.append(node)
            for child in children[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(order) != len(self.tasks):
            raise ValueError("Task graph contains a cycle.")
        return order

    # ── CLI entry point ───────────────────────────────────────────────

    def cli(self, argv: Optional[Sequence[str]] = None) -> int:
        """Run the Typer command-line interface.

        Parameters
        ----------
        argv : Sequence[str] or None
            Override ``sys.argv[1:]`` for testing.

        Returns
        -------
        int
            Exit code.
        """
        from .cli import build_app

        app = build_app(self)
        try:
            app(standalone_mode=False, args=list(argv) if argv is not None else None)
        except SystemExit as exc:
            return int(exc.code or 0)
        return 0

    # ── submit ────────────────────────────────────────────────────────

    def submit_run(
        self,
        run_dir: Path,
        parameters: Dict[str, Any],
        db_type: str = "sqlite",
        db_url: Optional[str] = None,
        executor: Optional[Executor] = None,
    ) -> str:
        """Create a new run and seed the manifest.

        Parameters
        ----------
        run_dir : Path
            Shared working directory for this run.
        parameters : Dict[str, Any]
            CLI parameters to store and inject into task functions.
        db_type : str
            Database backend type.
        db_url : str or None
            Explicit SQLAlchemy URL.
        executor : Executor or None
            Workload-manager executor.  Defaults to
            :class:`~slurm_flow.executors.slurm.SlurmExecutor`.

        Returns
        -------
        str
            The new ``run_id``.
        """
        run_dir = run_dir.expanduser().resolve()
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)

        resolved_url = _db.build_db_url(run_dir, db_type=db_type, db_url=db_url)
        engine = _db.connect_db(resolved_url)
        _db.init_db(engine)

        run_id = uuid.uuid4().hex

        # normalise and store resolved connection info
        params = dict(parameters)
        params["db_type"] = _db.normalize_db_type(
            str(params.get("db_type", db_type))
        )
        params["db_url"] = resolved_url
        params["run_dir"] = str(run_dir)

        _db.insert_run(engine, run_id, self.name, params)

        # persist task specs and dependencies
        for spec in self.tasks.values():
            _db.insert_task_spec(
                engine=engine,
                run_id=run_id,
                task_name=spec.name,
                is_array=spec.config.array,
                expand_expr=spec.config.expand,
                config_json=asdict(spec.config),
                dependencies=self._effective_dependencies(spec),
            )

        # seed instances for root tasks (no dependencies, no expansion)
        for spec in self.tasks.values():
            if self._effective_dependencies(spec) or spec.config.array:
                continue
            _db.insert_task_instance(
                engine=engine,
                run_id=run_id,
                task_name=spec.name,
                array_index=None,
                state=TaskState.PENDING,
                input_payload={},
            )

        # kick off the dispatcher
        exc = executor or SlurmExecutor.from_environment()
        dispatch_job_id = self._submit_dispatch(
            executor=exc,
            run_id=run_id,
            run_dir=run_dir,
            db_url=resolved_url,
            db_type=params["db_type"],
        )

        engine.dispose()
        logger.info(
            "Created run %s in %s (dispatch=%s)", run_id, run_dir, dispatch_job_id
        )
        return run_id

    # ── dispatch ──────────────────────────────────────────────────────

    def dispatch(
        self,
        run_id: str,
        run_dir: Path,
        db_url: Optional[str] = None,
        db_type: str = "sqlite",
        executor: Optional[Executor] = None,
    ) -> None:
        """Inspect the manifest and submit all runnable tasks.

        Parameters
        ----------
        run_id : str
            Run identifier.
        run_dir : Path
            Shared working directory.
        db_url : str or None
            Explicit SQLAlchemy URL.
        db_type : str
            Database backend type.
        executor : Executor or None
            Workload-manager executor.
        """
        resolved_url = _db.build_db_url(run_dir, db_type=db_type, db_url=db_url)
        exc = executor or SlurmExecutor.from_environment()

        for task_name in self._topological_order():
            spec = self.tasks[task_name]
            if spec.config.array:
                self._maybe_expand_and_submit_array(
                    run_id, run_dir, spec, resolved_url, exc
                )
            else:
                self._maybe_submit_single(
                    run_id, run_dir, spec, resolved_url, exc
                )

        # check whether the whole run is finished
        self._maybe_finalise_run(run_id, resolved_url)

    def _deps_satisfied(
        self,
        engine: Any,
        run_id: str,
        spec: TaskSpec,
    ) -> bool:
        """Check whether all dependencies of *spec* are satisfied."""
        for dep in self._effective_dependencies(spec):
            if not _db.dependency_is_satisfied(engine, run_id, dep):
                return False
        return True

    def _maybe_submit_single(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        db_url: str,
        executor: Executor,
    ) -> None:
        """Submit a singleton task if its dependencies are met."""
        engine = _db.connect_db(db_url)
        try:
            if not self._deps_satisfied(engine, run_id, spec):
                return

            row = _db.get_task_instance(engine, run_id, spec.name, None)
            if row is None:
                _db.insert_task_instance(
                    engine, run_id, spec.name, None, TaskState.PENDING, {}
                )
                row = _db.get_task_instance(engine, run_id, spec.name, None)

            if row is None:
                raise RuntimeError(
                    f"Could not create instance for {spec.name!r}."
                )
            state = TaskState(str(row["state"]))
            if state not in (TaskState.PENDING, TaskState.RETRYING):
                return

            job_id = executor.submit(
                resources=self._single_resources(run_dir, spec),
                command=self._worker_command(
                    run_id, run_dir, spec.name, db_url
                ),
            )
            _db.update_task_submitted(engine, run_id, spec.name, job_id)
        finally:
            engine.dispose()

    def _maybe_expand_and_submit_array(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        db_url: str,
        executor: Executor,
    ) -> None:
        """Expand an array task from upstream output and submit."""
        if not spec.config.expand:
            raise ValueError(
                f"Array task {spec.name!r} has no expand expression."
            )
        engine = _db.connect_db(db_url)
        try:
            if not self._deps_satisfied(engine, run_id, spec):
                return

            # check whether any retrying instances exist
            existing = _db.list_task_instances(
                engine, run_id, task_name=spec.name
            )
            retrying = [
                r for r in existing
                if TaskState(str(r["state"])) == TaskState.RETRYING
            ]
            if retrying:
                # resubmit only the retrying instances
                max_idx = max(int(r["array_index"]) for r in retrying)
                array_value = ",".join(
                    str(int(r["array_index"])) for r in retrying
                )
                job_id = executor.submit(
                    resources=self._array_resources(
                        run_dir, spec, array_value=array_value
                    ),
                    command=self._worker_command(
                        run_id, run_dir, spec.name, db_url
                    ),
                )
                _db.update_task_submitted(engine, run_id, spec.name, job_id)
                return

            # first expansion: no instances yet
            if _db.count_task_instances(engine, run_id, spec.name) > 0:
                return

            upstream_task, output_key = self._split_expand(spec.config.expand)
            output = _db.get_successful_singleton_output(
                engine, run_id, upstream_task
            )
            if output is None:
                return
            if output_key not in output:
                raise KeyError(
                    f"Key {output_key!r} missing in output of {upstream_task!r}."
                )

            items = output[output_key]
            if not isinstance(items, list):
                raise TypeError(
                    f"Expansion value for {spec.config.expand!r} must be a "
                    f"list, got {type(items).__name__!r}."
                )

            for idx, item in enumerate(items):
                payload = item if isinstance(item, dict) else {"item": item}
                _db.insert_task_instance(
                    engine, run_id, spec.name, idx, TaskState.PENDING, payload
                )

            if not items:
                return

            array_str = f"0-{len(items) - 1}"
            if spec.config.array_parallelism is not None:
                array_str = f"{array_str}%{spec.config.array_parallelism}"

            job_id = executor.submit(
                resources=self._array_resources(
                    run_dir, spec, array_value=array_str
                ),
                command=self._worker_command(
                    run_id, run_dir, spec.name, db_url
                ),
            )
            _db.update_task_submitted(engine, run_id, spec.name, job_id)
        finally:
            engine.dispose()

    def _maybe_finalise_run(self, run_id: str, db_url: str) -> None:
        """Set the run status if all tasks are terminal."""
        engine = _db.connect_db(db_url)
        try:
            summary = _db.task_state_summary(engine, run_id)
            if not summary:
                return

            all_success = True
            any_failed = False
            any_active = False

            for task_states in summary.values():
                for state_str, count in task_states.items():
                    state = TaskState(state_str)
                    if state in TaskState.active():
                        any_active = True
                    if state == TaskState.FAILED:
                        any_failed = True
                    if state != TaskState.SUCCESS:
                        all_success = False

            if any_active:
                return

            if all_success:
                _db.update_run_status(engine, run_id, RunState.SUCCESS)
            elif any_failed:
                _db.update_run_status(engine, run_id, RunState.FAILED)
        finally:
            engine.dispose()

    # ── worker ────────────────────────────────────────────────────────

    def worker(
        self,
        run_id: str,
        run_dir: Path,
        task_name: str,
        index: Optional[int] = None,
        db_url: Optional[str] = None,
        db_type: str = "sqlite",
        executor: Optional[Executor] = None,
    ) -> None:
        """Execute one task instance and re-dispatch.

        Parameters
        ----------
        run_id : str
            Run identifier.
        run_dir : Path
            Shared working directory.
        task_name : str
            Task to execute.
        index : int or None
            Explicit array index.  When ``None``, read from
            ``SLURM_ARRAY_TASK_ID``.
        db_url : str or None
            Explicit SQLAlchemy URL.
        db_type : str
            Database backend type.
        executor : Executor or None
            Workload-manager executor for the follow-up dispatch.
        """
        if task_name not in self.tasks:
            raise KeyError(f"Unknown task: {task_name!r}")

        resolved_index = self._resolve_index(index)
        spec = self.tasks[task_name]
        resolved_url = _db.build_db_url(
            run_dir, db_type=db_type, db_url=db_url
        )

        engine = _db.connect_db(resolved_url)
        try:
            row = _db.get_task_instance(
                engine, run_id, task_name, resolved_index
            )
            if row is None:
                raise KeyError(
                    f"Instance not found: run_id={run_id!r}, task={task_name!r}, "
                    f"index={resolved_index!r}"
                )

            parameters = _db.get_run_parameters(engine, run_id)
            task_input = _decode_json(row.get("input_json"))
            kwargs = self._build_kwargs(spec, parameters, task_input)

            instance_id = int(row["id"])
            _db.update_task_running(engine, instance_id)

            try:
                result = spec.func(**kwargs)
                if result is None:
                    payload: Dict[str, Any] = {}
                elif isinstance(result, dict):
                    payload = result
                else:
                    raise TypeError(
                        f"Task {task_name!r} must return a dict or None, "
                        f"got {type(result).__name__!r}."
                    )
                _db.update_task_success(engine, instance_id, payload)
            except Exception:
                _db.update_task_failed(engine, instance_id, traceback.format_exc())
                raise
        finally:
            engine.dispose()

        # re-dispatch so the DAG keeps progressing
        db_type_resolved = _db.normalize_db_type(db_type)
        exc = executor or SlurmExecutor.from_environment()
        self._submit_dispatch(
            executor=exc,
            run_id=run_id,
            run_dir=run_dir,
            db_url=resolved_url,
            db_type=db_type_resolved,
        )

    # ── cancel ────────────────────────────────────────────────────────

    def cancel_run(
        self,
        run_id: str,
        run_dir: Path,
        db_url: Optional[str] = None,
        db_type: str = "sqlite",
        task_name: Optional[str] = None,
        executor: Optional[Executor] = None,
    ) -> int:
        """Cancel active task instances for a run.

        Parameters
        ----------
        run_id : str
            Run identifier.
        run_dir : Path
            Shared working directory.
        db_url : str or None
            Explicit SQLAlchemy URL.
        db_type : str
            Database backend type.
        task_name : str or None
            If given, cancel only this task.  Otherwise cancel all.
        executor : Executor or None
            Workload-manager executor.

        Returns
        -------
        int
            Number of instances cancelled.
        """
        resolved_url = _db.build_db_url(
            run_dir, db_type=db_type, db_url=db_url
        )
        exc = executor or SlurmExecutor.from_environment()
        engine = _db.connect_db(resolved_url)
        cancelled = 0

        try:
            instances = _db.list_task_instances(
                engine,
                run_id,
                task_name=task_name,
                states=list(TaskState.cancellable()),
            )
            seen_jobs: Set[str] = set()
            for inst in instances:
                job_id = inst.get("slurm_job_id")
                if job_id and str(job_id) not in seen_jobs:
                    exc.cancel(str(job_id))
                    seen_jobs.add(str(job_id))
                _db.update_task_cancelled(engine, int(inst["id"]))
                cancelled += 1

            if task_name is None:
                _db.update_run_status(engine, run_id, RunState.CANCELLED)
        finally:
            engine.dispose()

        logger.info("Cancelled %d instances for run %s", cancelled, run_id)
        return cancelled

    # ── retry ─────────────────────────────────────────────────────────

    def retry_failed(
        self,
        run_id: str,
        run_dir: Path,
        db_url: Optional[str] = None,
        db_type: str = "sqlite",
        task_name: Optional[str] = None,
        executor: Optional[Executor] = None,
    ) -> int:
        """Mark failed instances for retry and re-dispatch.

        Parameters
        ----------
        run_id : str
            Run identifier.
        run_dir : Path
            Shared working directory.
        db_url : str or None
            Explicit SQLAlchemy URL.
        db_type : str
            Database backend type.
        task_name : str or None
            If given, retry only this task.
        executor : Executor or None
            Workload-manager executor.

        Returns
        -------
        int
            Number of instances marked for retry.
        """
        resolved_url = _db.build_db_url(
            run_dir, db_type=db_type, db_url=db_url
        )
        engine = _db.connect_db(resolved_url)
        retried = 0

        try:
            instances = _db.list_task_instances(
                engine,
                run_id,
                task_name=task_name,
                states=list(TaskState.retriable()),
            )
            for inst in instances:
                _db.mark_for_retry(engine, int(inst["id"]))
                retried += 1
            _db.update_run_status(engine, run_id, RunState.RUNNING)
        finally:
            engine.dispose()

        if retried > 0:
            exc = executor or SlurmExecutor.from_environment()
            db_type_resolved = _db.normalize_db_type(db_type)
            self._submit_dispatch(
                executor=exc,
                run_id=run_id,
                run_dir=run_dir,
                db_url=resolved_url,
                db_type=db_type_resolved,
            )
        logger.info("Marked %d instances for retry (run %s)", retried, run_id)
        return retried

    # ── status ────────────────────────────────────────────────────────

    def run_status(
        self,
        run_id: str,
        run_dir: Path,
        db_url: Optional[str] = None,
        db_type: str = "sqlite",
    ) -> Dict[str, Any]:
        """Query the current status of a run.

        Parameters
        ----------
        run_id : str
            Run identifier.
        run_dir : Path
            Shared working directory.
        db_url : str or None
            Explicit SQLAlchemy URL.
        db_type : str
            Database backend type.

        Returns
        -------
        Dict[str, Any]
            Dictionary with keys ``"run"``, ``"summary"``, ``"instances"``.

        Raises
        ------
        KeyError
            If *run_id* is not found.
        """
        resolved_url = _db.build_db_url(
            run_dir, db_type=db_type, db_url=db_url
        )
        engine = _db.connect_db(resolved_url)
        try:
            run_row = _db.get_run(engine, run_id)
            if run_row is None:
                raise KeyError(f"Unknown run_id: {run_id!r}")
            summary = _db.task_state_summary(engine, run_id)
            instances = _db.list_task_instances(engine, run_id)
        finally:
            engine.dispose()

        return {
            "run": dict(run_row),
            "summary": summary,
            "instances": [dict(i) for i in instances],
        }

    # ── helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _split_expand(expression: str) -> Tuple[str, str]:
        """Split ``"task.key"`` into ``("task", "key")``.

        Parameters
        ----------
        expression : str
            Expansion expression.

        Returns
        -------
        Tuple[str, str]
            ``(upstream_task, output_key)``.

        Raises
        ------
        ValueError
            If the expression is malformed.
        """
        parts = expression.split(".", 1)
        if len(parts) != 2:
            raise ValueError(
                f"Invalid expand expression {expression!r}.  "
                f"Expected 'task.output_key'."
            )
        return (parts[0], parts[1])

    @staticmethod
    def _resolve_index(explicit: Optional[int]) -> Optional[int]:
        """Resolve the array index from an explicit value or env."""
        if explicit is not None:
            return explicit
        env_val = os.getenv("SLURM_ARRAY_TASK_ID")
        return int(env_val) if env_val is not None else None

    @staticmethod
    def _build_kwargs(
        spec: TaskSpec,
        run_parameters: Dict[str, Any],
        task_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build function kwargs from run params and task input."""
        kwargs: Dict[str, Any] = {}
        for param_name in spec.signature.parameters:
            if param_name in task_input:
                kwargs[param_name] = task_input[param_name]
            elif param_name in run_parameters:
                kwargs[param_name] = run_parameters[param_name]
        return kwargs

    def _entrypoint(self) -> Path:
        """Path to the current Python script."""
        return Path(sys.argv[0]).expanduser().resolve()

    def _worker_command(
        self,
        run_id: str,
        run_dir: Path,
        task_name: str,
        db_url: str,
    ) -> List[str]:
        """Build the full worker command line."""
        python = os.getenv("SLURM_FLOW_PYTHON", sys.executable)
        return [
            python,
            str(self._entrypoint()),
            "worker",
            "--run-id", run_id,
            "--run-dir", str(run_dir),
            "--db-type", _db.infer_db_type(db_url),
            "--db-url", db_url,
            "--task", task_name,
        ]

    def _submit_dispatch(
        self,
        executor: Executor,
        run_id: str,
        run_dir: Path,
        db_url: str,
        db_type: str,
    ) -> str:
        """Submit a dispatcher job."""
        python = os.getenv("SLURM_FLOW_PYTHON", sys.executable)
        resources = JobResources(
            job_name=f"{self.name}-dispatch",
            cpus=1,
            time_limit="00:10:00",
            mem="1G",
            partition=os.getenv("SLURM_FLOW_DISPATCH_PARTITION", "compute"),
            account=os.getenv("SLURM_FLOW_DISPATCH_ACCOUNT"),
            output_path=run_dir / "logs" / "dispatch-%j.out",
            error_path=run_dir / "logs" / "dispatch-%j.err",
        )
        return executor.submit(
            resources=resources,
            command=[
                python,
                str(self._entrypoint()),
                "dispatch",
                "--run-id", run_id,
                "--run-dir", str(run_dir),
                "--db-type", db_type,
                "--db-url", db_url,
            ],
        )

    def _single_resources(
        self,
        run_dir: Path,
        spec: TaskSpec,
    ) -> JobResources:
        """Build resources for a singleton task."""
        return JobResources(
            job_name=f"{self.name}-{spec.name}",
            cpus=spec.config.cpus,
            time_limit=spec.config.time,
            mem=spec.config.mem,
            partition=spec.config.partition,
            account=spec.config.account,
            output_path=run_dir / "logs" / f"{spec.name}-%j.out",
            error_path=run_dir / "logs" / f"{spec.name}-%j.err",
            extra=spec.config.extra,
        )

    def _array_resources(
        self,
        run_dir: Path,
        spec: TaskSpec,
        array_value: str,
    ) -> JobResources:
        """Build resources for an array task."""
        return JobResources(
            job_name=f"{self.name}-{spec.name}",
            cpus=spec.config.cpus,
            time_limit=spec.config.time,
            mem=spec.config.mem,
            partition=spec.config.partition,
            account=spec.config.account,
            array=array_value,
            output_path=run_dir / "logs" / f"{spec.name}-%A_%a.out",
            error_path=run_dir / "logs" / f"{spec.name}-%A_%a.err",
            extra=spec.config.extra,
        )


# ── module-level helpers ──────────────────────────────────────────────


def _decode_json(value: Any) -> Dict[str, Any]:
    """Decode a JSON column value to a dict."""
    if value is None:
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)
