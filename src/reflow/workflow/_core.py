"""Core Workflow class for reflow.

[`Workflow`][reflow.Workflow] extends [`Flow`][Flow] with validation,
submission, CLI integration, cancellation, retry, status queries,
and workflow description.  Dispatch and worker logic live in the
:mod:`._dispatch` and :mod:`._worker` mixins.
"""

from __future__ import annotations

import difflib
import getpass
import inspect
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, get_type_hints

from .._types import RunState, TaskState
from ..config import Config, load_config
from ..executors import Executor, JobResources
from ..flow import Flow, TaskSpec
from ..manifest import (
    CliParamDescription,
    TaskDescription,
    WorkflowDescription,
)
from ..params import (
    check_type_compatibility,
    collect_cli_params,
    extract_base_type,
    merge_resolved_params,
)
from ..run import Run
from ..stores import Store
from ..stores.sqlite import SqliteStore
from ._dispatch import DispatchMixin
from ._helpers import default_executor, make_run_id, resolve_executor
from ._worker import WorkerMixin

logger = logging.getLogger(__name__)


def _suggest_name(unknown: str, known: set[str] | dict[str, Any]) -> str:
    """Return a 'did you mean ...' hint, or empty string."""
    matches = difflib.get_close_matches(unknown, list(known), n=1, cutoff=0.6)
    if matches:
        return f"  Did you mean {matches[0]!r}?"
    return ""


class Workflow(DispatchMixin, WorkerMixin, Flow):
    """Runnable HPC workflow.

    Extends [`Flow`][Flow] with execution: validation, CLI, submission,
    dispatch, worker, cancel, retry, status.

    Parameters
    ----------
    name : str
        Workflow name.
    config : Config or None
        User configuration.  Loaded from
        ``~/.config/reflow/config.toml`` when ``None``.

    Examples
    --------
    Direct task registration::

        wf = Workflow("pipeline")

        @wf.job()
        def prepare(...) -> list[str]: ...

        wf.cli()

    Composing from flows::

        conversion = Flow("conversion")
        # ... tasks ...

        wf = Workflow("experiment")
        wf.include(conversion)
        run = wf.submit(run_dir="/scratch/run1", start="2025-01-01")
        run.status()

    """

    def __init__(self, name: str, config: Config | None = None) -> None:
        super().__init__(name)
        self.config: Config = config or load_config()

    # --- include -----------------------------------------------------------

    def include(self, flow: Flow, prefix: str | None = None) -> None:
        """Include all tasks from a flow into this workflow.

        Parameters
        ----------
        flow : Flow
            Reusable flow.
        prefix : str or None
            Optional name prefix.  Internal Result/after references
            within the flow are rewritten automatically.

        """
        new_tasks, new_order = flow._prefixed_tasks(prefix)
        for task_name, spec in new_tasks.items():
            if task_name in self.tasks:
                raise ValueError(
                    f"Cannot include flow {flow.name!r}: task "
                    f"{task_name!r} already exists."
                )
            self.tasks[task_name] = spec
        self._registration_order.extend(new_order)

    # --- validation --------------------------------------------------------

    def validate(self) -> None:
        """Run full graph validation.

        Raises
        ------
        ValueError
            Unresolved references or cycles.
        TypeError
            Incompatible wired types.

        """
        for spec in self.tasks.values():
            for pname, result in spec.result_deps.items():
                for step in result.steps:
                    if step not in self.tasks:
                        hint = _suggest_name(step, self.tasks)
                        raise ValueError(
                            f"Task {spec.name!r} param {pname!r} "
                            f"references unknown task {step!r}.{hint}"
                        )
            for dep in spec.config.after:
                if dep not in self.tasks:
                    hint = _suggest_name(dep, self.tasks)
                    raise ValueError(
                        f"Task {spec.name!r} after=[{dep!r}] "
                        f"references unknown task {dep!r}.{hint}"
                    )

        for spec in self.tasks.values():
            try:
                hints = get_type_hints(spec.func, include_extras=True)
            except Exception:
                continue
            for pname, result in spec.result_deps.items():
                raw_ann = hints.get(pname)
                if raw_ann is None:
                    continue
                param_type = extract_base_type(raw_ann)
                for step in result.steps:
                    upstream = self.tasks[step]
                    if upstream.return_type in (None, inspect.Parameter.empty):
                        continue
                    check_type_compatibility(
                        upstream.return_type,
                        upstream.config.array,
                        param_type,
                        spec.config.array,
                        step,
                        spec.name,
                        pname,
                        broadcast=result.broadcast,
                    )

        self._topological_order()

    # --- dependency resolution ---------------------------------------------

    def _effective_dependencies(self, spec: TaskSpec) -> list[str]:
        deps: set[str] = set(spec.config.after)
        for result in spec.result_deps.values():
            deps.update(result.steps)
        return sorted(deps)

    def _topological_order(self) -> list[str]:
        in_degree: dict[str, int] = {n: 0 for n in self.tasks}
        children: dict[str, list[str]] = {n: [] for n in self.tasks}
        for name, spec in self.tasks.items():
            for dep in self._effective_dependencies(spec):
                if dep not in self.tasks:
                    hint = _suggest_name(dep, self.tasks)
                    raise ValueError(f"Task {name!r} depends on unknown {dep!r}.{hint}")
                in_degree[name] += 1
                children[dep].append(name)

        queue = [n for n in self._registration_order if in_degree[n] == 0]
        order: list[str] = []
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

    # --- CLI ---------------------------------------------------------------

    def cli(self, argv: Sequence[str] | None = None) -> int:
        """Run the argparse CLI."""
        self.validate()
        from ..cli import parse_args, run_command

        args = parse_args(self, list(argv) if argv is not None else None)
        return run_command(self, args)

    # --- submit (kwargs convenience) ---------------------------------------

    def submit(
        self,
        run_dir: str | Path,
        *,
        store: Store | None = None,
        executor: Executor | str | None = None,
        force: bool = False,
        force_tasks: list[str] | None = None,
        verify: bool = False,
        **parameters: Any,
    ) -> Run:
        """Create a new run and return an interactive [`Run`][Run] handle.

        Parameters
        ----------
        run_dir : str or Path
            Shared working directory.
        store : Store or None
            Manifest store. Defaults to the shared
            [`SqliteStore`][reflow.stores.sqlite.SqliteStore] in the user
            cache directory.
        executor : Executor, str, or None
            Workload-manager executor.  ``"local"`` is a shorthand
            for [`reflow.LocalExecutor`][reflow.LocalExecutor].
        force : bool
            If ``True``, skip the Merkle cache entirely and run all
            tasks fresh.
        force_tasks : list[str] or None
            Task names to force-run even if cached.  Other tasks
            may still use the cache.
        verify : bool
            If ``True``, run output verification (file existence for
            ``Path`` outputs, custom callables) on every cache hit.
            By default the Merkle identity is trusted on submit;
            verification runs automatically during retry.
        **parameters
            Run parameters injected into task functions by name.

        Returns
        -------
        Run

        Raises
        ------
        TypeError
            If required parameters are missing.

        """
        real_executor = resolve_executor(executor)
        self.validate()
        self._check_submit_params(parameters)

        rd = Path(run_dir).expanduser().resolve()
        rd.mkdir(parents=True, exist_ok=True)
        (rd / "logs").mkdir(parents=True, exist_ok=True)

        st = store or SqliteStore.default(self.config)
        st.init()

        run_id = make_run_id(self.name)
        user_id = getpass.getuser()

        params = dict(parameters)
        params["run_dir"] = str(rd)
        if force:
            params["__force__"] = True
        if force_tasks:
            params["__force_tasks__"] = force_tasks

        st.insert_run(run_id, self.name, user_id, params)

        for spec in self.tasks.values():
            st.insert_task_spec(
                run_id,
                spec.name,
                spec.config.array,
                asdict(spec.config),
                self._effective_dependencies(spec),
            )

        for spec in self.tasks.values():
            if self._effective_dependencies(spec) or spec.config.array:
                continue
            st.insert_task_instance(
                run_id,
                spec.name,
                None,
                TaskState.PENDING,
                {},
            )

        exc = real_executor or default_executor(self.config)
        self._submit_dispatch(exc, run_id, rd, st, verify=verify)

        logger.info("Created run %s in %s", run_id, rd)
        return Run(workflow=self, run_id=run_id, run_dir=rd, store=st)

    def _check_submit_params(self, parameters: dict[str, Any]) -> None:
        all_resolved = []
        for spec in self.tasks.values():
            all_resolved.extend(
                collect_cli_params(spec.name, spec.func, spec.signature)
            )
        merged = merge_resolved_params(all_resolved)
        missing = [
            rp.cli_flag()
            for rp in merged
            if rp.required
            and (rp.dest_name() if rp.namespace == "local" else rp.name)
            not in parameters
        ]
        if missing:
            raise TypeError(
                f"submit() missing required parameter(s): {', '.join(missing)}"
            )

    # --- submit_run (lower-level, for CLI) ---------------------------------

    def submit_run(
        self,
        run_dir: Path,
        parameters: dict[str, Any],
        store: Store | None = None,
        executor: Executor | None = None,
    ) -> str:
        """Lower-level submission used by the CLI.  Returns run_id."""
        self.validate()
        rd = Path(run_dir).expanduser().resolve()
        rd.mkdir(parents=True, exist_ok=True)
        (rd / "logs").mkdir(parents=True, exist_ok=True)

        st = store or SqliteStore.default(self.config)
        st.init()

        run_id = make_run_id(self.name)
        user_id = getpass.getuser()

        params = dict(parameters)
        params["run_dir"] = str(rd)

        st.insert_run(run_id, self.name, user_id, params)

        for spec in self.tasks.values():
            st.insert_task_spec(
                run_id,
                spec.name,
                spec.config.array,
                asdict(spec.config),
                self._effective_dependencies(spec),
            )

        for spec in self.tasks.values():
            if self._effective_dependencies(spec) or spec.config.array:
                continue
            st.insert_task_instance(
                run_id,
                spec.name,
                None,
                TaskState.PENDING,
                {},
            )

        exc = executor or default_executor(self.config)
        self._submit_dispatch(exc, run_id, rd, st)
        return run_id

    # --- cancel / retry / status -------------------------------------------

    def cancel_run(
        self,
        run_id: str,
        store: Store,
        task_name: str | None = None,
        executor: Executor | None = None,
    ) -> int:
        exc = executor or default_executor(self.config)
        instances = store.list_task_instances(
            run_id,
            task_name=task_name,
            states=list(TaskState.cancellable()),
        )
        seen: set[str] = set()
        cancelled = 0
        for inst in instances:
            jid = inst.get("job_id")
            if jid and str(jid) not in seen:
                exc.cancel(str(jid))
                seen.add(str(jid))
            store.update_task_cancelled(int(inst["id"]))
            cancelled += 1
        if task_name is None:
            store.update_run_status(run_id, RunState.CANCELLED)
        return cancelled

    def retry_failed(
        self,
        run_id: str,
        store: Store,
        run_dir: Path,
        task_name: str | None = None,
        executor: Executor | None = None,
        verify: bool = True,
    ) -> int:
        """Retry failed/cancelled instances and re-dispatch.

        Parameters
        ----------
        run_id : str
            Run identifier.
        store : Store
            Manifest store for the run.
        run_dir : Path
            Working directory for the run.
        task_name : str or None
            Optional task name filter.
        executor : Executor or None
            Explicit executor override.
        verify : bool
            If ``True`` (the default for retry), verify cached
            upstream outputs before resubmitting.  This catches
            stale intermediate files that were deleted after the
            original run.

        """
        instances = store.list_task_instances(
            run_id,
            task_name=task_name,
            states=list(TaskState.retriable()),
        )
        retried = 0
        for inst in instances:
            store.mark_for_retry(int(inst["id"]))
            retried += 1
        store.update_run_status(run_id, RunState.RUNNING)
        if retried > 0:
            exc = executor or default_executor(self.config)
            self._submit_dispatch(exc, run_id, run_dir, store, verify=verify)
        return retried

    def run_status(self, run_id: str, store: Store) -> dict[str, Any]:
        run_row = store.get_run(run_id)
        if run_row is None:
            raise KeyError(f"Unknown run_id: {run_id!r}")
        return {
            "run": run_row,
            "summary": store.task_state_summary(run_id),
            "instances": store.list_task_instances(run_id),
        }

    # --- describe (for future server registration) -------------------------

    def describe_typed(self) -> WorkflowDescription:
        """Return a typed workflow description.

        This is the canonical manifest description used by external tooling and
        future service integrations.  [`describe`][describe] converts it into a
        JSON-safe dictionary.
        """
        self.validate()
        tasks_desc: list[TaskDescription] = []
        for name in self._topological_order():
            spec = self.tasks[name]
            tasks_desc.append(
                TaskDescription(
                    name=spec.name,
                    is_array=spec.config.array,
                    config=asdict(spec.config),
                    dependencies=self._effective_dependencies(spec),
                    result_deps={
                        pname: {"steps": result.steps}
                        for pname, result in spec.result_deps.items()
                    },
                    return_type=str(spec.return_type) if spec.return_type else None,
                    has_verify=spec.verify is not None,
                )
            )

        cli_params: list[CliParamDescription] = []
        all_resolved = []
        for spec in self.tasks.values():
            all_resolved.extend(
                collect_cli_params(spec.name, spec.func, spec.signature)
            )
        for rp in merge_resolved_params(all_resolved):
            cli_params.append(
                CliParamDescription(
                    name=rp.name,
                    task=rp.task_name,
                    flag=rp.cli_flag(),
                    type_repr=str(rp.base_type),
                    required=rp.required,
                    default=rp.default,
                    namespace=rp.namespace,
                    help=rp.help_text,
                    choices=list(rp.literal_choices) if rp.literal_choices else None,
                )
            )

        return WorkflowDescription(
            name=self.name,
            entrypoint=Path(sys.argv[0]).expanduser().resolve(),
            python=Path(sys.executable),
            working_dir=Path.cwd(),
            tasks=tasks_desc,
            cli_params=cli_params,
        )

    def describe(self) -> dict[str, Any]:
        """Return a JSON-safe description of the workflow.

        Includes task names, types, resource configs, dependencies, and CLI
        parameters -- everything a server would need to reconstruct the submit
        form and dispatch logic.
        """
        return self.describe_typed().to_manifest_dict()

    # --- internal helpers --------------------------------------------------

    def _entrypoint(self) -> Path:
        return Path(sys.argv[0]).expanduser().resolve()

    def _worker_command(
        self,
        run_id: str,
        run_dir: Path,
        task_name: str,
        store: Store,
    ) -> list[str]:
        python = os.getenv("REFLOW_PYTHON") or sys.executable
        cmd = [
            python,
            str(self._entrypoint()),
            "worker",
            "--run-id",
            run_id,
            "--run-dir",
            str(run_dir),
            "--task",
            task_name,
        ]
        if isinstance(store, SqliteStore):
            cmd.extend(["--store-path", str(store.path)])
        return cmd

    def _submit_dispatch(
        self,
        executor: Executor,
        run_id: str,
        run_dir: Path,
        store: Store,
        verify: bool = False,
        dependency_options: dict[str, str] | None = None,
    ) -> str:
        """Submit a dispatch job, optionally with scheduler dependencies."""
        python = (
            os.getenv("REFLOW_PYTHON") or self.config.executor_python or sys.executable
        )
        submit_options = dict(self.config.dispatch_submit_options)
        if dependency_options:
            submit_options.update(dependency_options)
        resources = JobResources(
            job_name=f"{self.name}-dispatch",
            cpus=int(self.config.dispatch_cpus or "1"),
            time_limit=self.config.dispatch_time or "00:10:00",
            mem=self.config.dispatch_mem or "1G",
            submit_options=submit_options,
            output_path=run_dir / "logs" / "dispatch-%j.out",
            error_path=run_dir / "logs" / "dispatch-%j.err",
        )
        cmd = [
            python,
            str(self._entrypoint()),
            "dispatch",
            "--run-id",
            run_id,
            "--run-dir",
            str(run_dir),
        ]
        if isinstance(store, SqliteStore):
            cmd.extend(["--store-path", str(store.path)])
        if verify:
            cmd.append("--verify")
        return executor.submit(resources, cmd)

    def _single_resources(self, run_dir: Path, spec: TaskSpec) -> JobResources:
        submit_options = dict(self.config.executor_submit_options)
        submit_options.update(spec.config.submit_options)
        return JobResources(
            job_name=f"{self.name}-{spec.name}",
            cpus=spec.config.cpus,
            time_limit=spec.config.time,
            mem=spec.config.mem,
            submit_options=submit_options,
            output_path=run_dir / "logs" / f"{spec.name}-%j.out",
            error_path=run_dir / "logs" / f"{spec.name}-%j.err",
            backend=spec.config.backend,
        )

    def _array_resources(
        self,
        run_dir: Path,
        spec: TaskSpec,
        array_value: str,
    ) -> JobResources:
        submit_options = dict(self.config.executor_submit_options)
        submit_options.update(spec.config.submit_options)
        return JobResources(
            job_name=f"{self.name}-{spec.name}",
            cpus=spec.config.cpus,
            time_limit=spec.config.time,
            mem=spec.config.mem,
            array=array_value,
            submit_options=submit_options,
            output_path=run_dir / "logs" / f"{spec.name}-%A_%a.out",
            error_path=run_dir / "logs" / f"{spec.name}-%A_%a.err",
            backend=spec.config.backend,
        )
