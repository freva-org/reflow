"""Runnable workflow (extends Flow with execution machinery).

[`Workflow`][reflow.Workflow] adds validation, submission, dispatch, worker
execution, cancellation, retry, and status queries to [`Flow`][Flow].
"""

from __future__ import annotations

import getpass
import inspect
import json
import logging
import os
import sys
import traceback
import uuid
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, get_type_hints

from ._types import RunState, TaskState
from .cache import (
    compute_identity,
    compute_input_hash,
    compute_output_hash,
    verify_cached_output,
)
from .config import Config, load_config
from .executors import Executor, JobResources
from .executors.slurm import SlurmExecutor
from .flow import Flow, TaskSpec
from .manifest import (
    CliParamDescription,
    TaskDescription,
    WorkflowDescription,
)
from .params import (
    WireMode,
    check_type_compatibility,
    collect_cli_params,
    extract_base_type,
    infer_wire_mode,
    is_run_dir,
    merge_resolved_params,
)
from .run import Run
from .stores import Store
from .stores.sqlite import SqliteStore

logger = logging.getLogger(__name__)


class Workflow(Flow):
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
                        raise ValueError(
                            f"Task {spec.name!r} param {pname!r} "
                            f"references unknown task {step!r}."
                        )
            for dep in spec.config.after:
                if dep not in self.tasks:
                    raise ValueError(
                        f"Task {spec.name!r} after=[{dep!r}] "
                        f"references unknown task {dep!r}."
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
                    raise ValueError(f"Task {name!r} depends on unknown {dep!r}.")
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
        from .cli import parse_args, run_command

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
        real_executor = _resolve_executor(executor)
        self.validate()
        self._check_submit_params(parameters)

        rd = Path(run_dir).expanduser().resolve()
        rd.mkdir(parents=True, exist_ok=True)
        (rd / "logs").mkdir(parents=True, exist_ok=True)

        st = store or SqliteStore.default(self.config)
        st.init()

        run_id = _make_run_id(self.name)
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

        exc = real_executor or SlurmExecutor.from_environment()
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

        run_id = _make_run_id(self.name)
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

        exc = executor or SlurmExecutor.from_environment()
        self._submit_dispatch(exc, run_id, rd, st)
        return run_id

    # --- dispatch ----------------------------------------------------------

    def dispatch(
        self,
        run_id: str,
        store: Store,
        run_dir: Path,
        executor: Executor | None = None,
        verify: bool = False,
    ) -> None:
        """Inspect the manifest and submit runnable tasks.

        Parameters
        ----------
        run_id : str
            Workflow run identifier.
        store : Store
            Manifest store used to read and update task state.
        run_dir : Path
            Run directory containing logs and task artefacts.
        executor : Executor or None
            Executor used for submission. ``None`` uses the default Slurm executor.
        verify : bool
            If ``True``, verify cached outputs (file existence for
            Path types, custom callables) before accepting cache hits.
            Default is ``False`` -- the Merkle identity is trusted.
            Set to ``True`` during retry to catch stale outputs.

        """
        exc = executor or SlurmExecutor.from_environment()
        for task_name in self._topological_order():
            spec = self.tasks[task_name]
            if spec.config.array:
                self._dispatch_array(run_id, run_dir, spec, store, exc, verify=verify)
            else:
                self._dispatch_single(run_id, run_dir, spec, store, exc, verify=verify)
        self._maybe_finalise_run(run_id, store)

    def _all_deps_satisfied(self, store: Store, run_id: str, spec: TaskSpec) -> bool:
        return all(
            store.dependency_is_satisfied(run_id, dep)
            for dep in self._effective_dependencies(spec)
        )

    def _resolve_result_inputs(
        self,
        store: Store,
        run_id: str,
        spec: TaskSpec,
    ) -> dict[str, Any]:
        try:
            hints = get_type_hints(spec.func, include_extras=True)
        except Exception:
            hints = {}

        resolved: dict[str, Any] = {}
        for pname, result in spec.result_deps.items():
            raw_ann = hints.get(pname)
            param_type = extract_base_type(raw_ann) if raw_ann else None

            all_values: list[Any] = []
            for step in result.steps:
                upstream = self.tasks[step]
                if upstream.config.array:
                    all_values.extend(store.get_array_outputs(run_id, step))
                else:
                    output = store.get_singleton_output(run_id, step)
                    if output is not None:
                        all_values.append(output)

            first_up = self.tasks[result.steps[0]]
            wire = None
            if param_type is not None and first_up.return_type not in (
                None,
                inspect.Parameter.empty,
            ):
                wire = infer_wire_mode(
                    first_up.return_type,
                    first_up.config.array,
                    param_type,
                    spec.config.array,
                )

            if wire == WireMode.DIRECT:
                resolved[pname] = all_values[0] if all_values else None
            elif wire in (
                WireMode.FAN_OUT,
                WireMode.CHAIN_FLATTEN,
                WireMode.GATHER_FLATTEN,
            ):
                flat: list[Any] = []
                for v in all_values:
                    flat.extend(v) if isinstance(v, list) else flat.append(v)
                resolved[pname] = flat
            elif wire == WireMode.GATHER:
                resolved[pname] = all_values
            elif wire == WireMode.CHAIN:
                resolved[pname] = all_values
            else:
                resolved[pname] = all_values[0] if len(all_values) == 1 else all_values

        return resolved

    def _find_fan_out_param(self, spec: TaskSpec) -> str | None:
        try:
            hints = get_type_hints(spec.func, include_extras=True)
        except Exception:
            return None
        for pname, result in spec.result_deps.items():
            raw_ann = hints.get(pname)
            if raw_ann is None:
                continue
            param_type = extract_base_type(raw_ann)
            first_up = self.tasks.get(result.steps[0])
            if first_up is None or first_up.return_type in (
                None,
                inspect.Parameter.empty,
            ):
                continue
            try:
                wire = infer_wire_mode(
                    first_up.return_type,
                    first_up.config.array,
                    param_type,
                    spec.config.array,
                )
            except TypeError:
                continue
            if wire in (WireMode.FAN_OUT, WireMode.CHAIN_FLATTEN):
                return pname
        return next(iter(spec.result_deps), None)

    def _should_force(self, store: Store, run_id: str, task_name: str) -> bool:
        """Check whether caching is bypassed for a task."""
        params = store.get_run_parameters(run_id)
        if params.get("__force__"):
            return True
        forced = params.get("__force_tasks__", [])
        return task_name in forced

    def _try_cache(
        self,
        store: Store,
        run_id: str,
        spec: TaskSpec,
        direct_inputs: dict[str, Any],
        array_index: int | None,
        upstream_output_hashes: list[str],
        verify: bool = False,
    ) -> bool:
        """Attempt to resolve a task from cache.

        If a cached result is found, creates a SUCCESS instance in
        the current run and returns True.

        Verification (checking file existence for Path outputs,
        calling custom verify callables) only runs when *verify*
        is ``True``.  By default the Merkle identity is trusted.

        Parameters
        ----------
        store : Store
            Manifest store.
        run_id : str
            Current run id.
        spec : TaskSpec
            Task specification.
        direct_inputs : dict[str, Any]
            The input payload for this instance.
        array_index : int or None
            Array index or None for singletons.
        upstream_output_hashes : list[str]
            Sorted output hashes from upstream dependencies.
        verify : bool
            If True, run output verification before accepting
            the cached result.

        Returns
        -------
        bool
            True if cache hit (instance created as SUCCESS).

        """
        if not spec.config.cache:
            return False
        if self._should_force(store, run_id, spec.name):
            return False

        input_hash = compute_input_hash(
            spec.name,
            spec.config.version,
            direct_inputs,
        )
        identity = compute_identity(input_hash, upstream_output_hashes)

        cached = store.find_cached(spec.name, identity)
        if cached is None:
            return False

        # Only verify when explicitly requested
        # (retry path, or submit with verify=True).
        if verify:
            if not verify_cached_output(
                cached["output"], spec.return_type, spec.verify
            ):
                logger.info("Cache stale for %s (identity=%s)", spec.name, identity)
                return False

        # Cache hit: create instance as SUCCESS with cached output.
        out_hash = cached.get("output_hash", "")
        if not out_hash and cached["output"] is not None:
            out_hash = compute_output_hash(cached["output"])

        iid = store.insert_task_instance(
            run_id,
            spec.name,
            array_index,
            TaskState.SUCCESS,
            direct_inputs,
            identity=identity,
            input_hash=input_hash,
        )
        store.update_task_success(iid, cached["output"], output_hash=out_hash)
        logger.info(
            "Cache hit for %s[%s] (identity=%s)", spec.name, array_index, identity
        )
        return True

    def _collect_upstream_output_hashes(
        self,
        store: Store,
        run_id: str,
        spec: TaskSpec,
    ) -> list[str]:
        """Collect output hashes from all upstream dependencies."""
        hashes: list[str] = []
        for dep_name in self._effective_dependencies(spec):
            dep_hashes = store.get_all_output_hashes(run_id, dep_name)
            hashes.extend(dep_hashes)
        return sorted(hashes)

    def _dispatch_single(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        store: Store,
        executor: Executor,
        verify: bool = False,
    ) -> None:
        if not self._all_deps_satisfied(store, run_id, spec):
            return
        row = store.get_task_instance(run_id, spec.name, None)
        if row is None:
            payload = self._resolve_result_inputs(store, run_id, spec)
            up_hashes = self._collect_upstream_output_hashes(store, run_id, spec)

            # Try cache before creating a PENDING instance.
            if self._try_cache(
                store, run_id, spec, payload, None, up_hashes, verify=verify
            ):
                return

            input_hash = compute_input_hash(spec.name, spec.config.version, payload)
            identity = compute_identity(input_hash, up_hashes)
            store.insert_task_instance(
                run_id,
                spec.name,
                None,
                TaskState.PENDING,
                payload,
                identity=identity,
                input_hash=input_hash,
            )
            row = store.get_task_instance(run_id, spec.name, None)
        if row is None:
            raise RuntimeError(f"Could not create instance for {spec.name!r}.")
        state = TaskState(str(row["state"]))
        if state not in (TaskState.PENDING, TaskState.RETRYING):
            return
        job_id = executor.submit(
            self._single_resources(run_dir, spec),
            self._worker_command(run_id, run_dir, spec.name, store),
        )
        store.update_task_submitted(run_id, spec.name, job_id)

    def _dispatch_array(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        store: Store,
        executor: Executor,
        verify: bool = False,
    ) -> None:
        if not self._all_deps_satisfied(store, run_id, spec):
            return

        existing = store.list_task_instances(run_id, task_name=spec.name)
        retrying = [
            r for r in existing if TaskState(str(r["state"])) == TaskState.RETRYING
        ]
        if retrying:
            arr = ",".join(str(int(r["array_index"])) for r in retrying)
            job_id = executor.submit(
                self._array_resources(run_dir, spec, arr),
                self._worker_command(run_id, run_dir, spec.name, store),
            )
            store.update_task_submitted(run_id, spec.name, job_id)
            return

        if store.count_task_instances(run_id, spec.name) > 0:
            return

        result_inputs = self._resolve_result_inputs(store, run_id, spec)
        if not result_inputs:
            return
        fan_param = self._find_fan_out_param(spec)
        if fan_param is None or fan_param not in result_inputs:
            return
        fan_items = result_inputs[fan_param]
        if not isinstance(fan_items, list):
            fan_items = [fan_items]
        if not fan_items:
            return

        up_hashes = self._collect_upstream_output_hashes(store, run_id, spec)
        pending_indices: list[int] = []

        for idx, element in enumerate(fan_items):
            payload: dict[str, Any] = {fan_param: element}
            for pname, value in result_inputs.items():
                if pname == fan_param:
                    continue
                if isinstance(value, list) and len(value) == len(fan_items):
                    payload[pname] = value[idx]
                else:
                    payload[pname] = value

            # Try cache for this individual array element.
            if self._try_cache(
                store, run_id, spec, payload, idx, up_hashes, verify=verify
            ):
                continue  # cache hit, no Slurm job needed

            input_hash = compute_input_hash(spec.name, spec.config.version, payload)
            identity = compute_identity(input_hash, up_hashes)
            store.insert_task_instance(
                run_id,
                spec.name,
                idx,
                TaskState.PENDING,
                payload,
                identity=identity,
                input_hash=input_hash,
            )
            pending_indices.append(idx)

        if not pending_indices:
            # All instances resolved from cache.
            logger.info(
                "All %d instances of %s resolved from cache.", len(fan_items), spec.name
            )
            return

        # Submit only the uncached indices.
        if len(pending_indices) == len(fan_items):
            arr_str = f"0-{len(fan_items) - 1}"
        else:
            arr_str = ",".join(str(i) for i in pending_indices)
        if spec.config.array_parallelism is not None:
            arr_str = f"{arr_str}%{spec.config.array_parallelism}"
        job_id = executor.submit(
            self._array_resources(run_dir, spec, arr_str),
            self._worker_command(run_id, run_dir, spec.name, store),
        )
        store.update_task_submitted(run_id, spec.name, job_id)

    def _maybe_finalise_run(self, run_id: str, store: Store) -> None:
        summary = store.task_state_summary(run_id)
        if not summary:
            return
        all_ok, any_fail, any_active = True, False, False
        for task_states in summary.values():
            for state_str in task_states:
                s = TaskState(state_str)
                if s in TaskState.active():
                    any_active = True
                if s == TaskState.FAILED:
                    any_fail = True
                if s != TaskState.SUCCESS:
                    all_ok = False
        if any_active:
            return
        if all_ok:
            store.update_run_status(run_id, RunState.SUCCESS)
        elif any_fail:
            store.update_run_status(run_id, RunState.FAILED)

    # --- worker ------------------------------------------------------------

    def worker(
        self,
        run_id: str,
        store: Store,
        run_dir: Path,
        task_name: str,
        index: int | None = None,
        executor: Executor | None = None,
    ) -> None:
        """Execute one task instance and re-dispatch.

        Signal handling: SIGTERM and SIGINT are converted to
        [`reflow.signals.TaskInterrupted`][reflow.signals.TaskInterrupted]
        so that the existing error handler stores the traceback and marks
        the instance as FAILED.
        """
        from .signals import graceful_shutdown

        if task_name not in self.tasks:
            raise KeyError(f"Unknown task: {task_name!r}")
        resolved_index = _resolve_index(index)
        spec = self.tasks[task_name]

        row = store.get_task_instance(run_id, task_name, resolved_index)
        if row is None:
            raise KeyError(
                f"Instance not found: {run_id!r}/{task_name!r}/{resolved_index!r}"
            )
        parameters = store.get_run_parameters(run_id)
        task_input = row.get("input", {})
        if isinstance(task_input, str):
            task_input = json.loads(task_input)
        kwargs = _build_kwargs(spec, parameters, task_input or {})

        instance_id = int(row["id"])
        store.update_task_running(instance_id)
        try:
            with graceful_shutdown():
                result = spec.func(**kwargs)
            out_hash = compute_output_hash(result)
            store.update_task_success(instance_id, result, output_hash=out_hash)
        except Exception:
            store.update_task_failed(instance_id, traceback.format_exc())
            raise

        exc = executor or SlurmExecutor.from_environment()
        self._submit_dispatch(exc, run_id, run_dir, store)

    # --- cancel / retry / status -------------------------------------------

    def cancel_run(
        self,
        run_id: str,
        store: Store,
        task_name: str | None = None,
        executor: Executor | None = None,
    ) -> int:
        """Cancel active instances for a run or a single task."""
        exc = executor or SlurmExecutor.from_environment()
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
            Workflow run identifier.
        store : Store
            Manifest store used to update retry state.
        run_dir : Path
            Run directory containing logs and task artefacts.
        task_name : str or None
            Restrict the retry to one task name. ``None`` retries all retriable tasks.
        executor : Executor or None
            Executor used for resubmission. ``None`` uses the default Slurm executor.
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
            exc = executor or SlurmExecutor.from_environment()
            self._submit_dispatch(exc, run_id, run_dir, store, verify=verify)
        return retried

    def run_status(self, run_id: str, store: Store) -> dict[str, Any]:
        """Return run metadata, task summary counts, and all task instances."""
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
        python = os.getenv("REFLOW_PYTHON", sys.executable)
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
    ) -> str:
        python = os.getenv("REFLOW_PYTHON", sys.executable)
        resources = JobResources(
            job_name=f"{self.name}-dispatch",
            cpus=1,
            time_limit="00:10:00",
            mem="1G",
            partition=os.getenv("REFLOW_DISPATCH_PARTITION", "compute"),
            account=os.getenv("REFLOW_DISPATCH_ACCOUNT"),
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
        cfg = self.config
        return JobResources(
            job_name=f"{self.name}-{spec.name}",
            cpus=spec.config.cpus,
            time_limit=spec.config.time,
            mem=spec.config.mem,
            partition=spec.config.partition,
            account=spec.config.account or cfg.executor_account,
            output_path=run_dir / "logs" / f"{spec.name}-%j.out",
            error_path=run_dir / "logs" / f"{spec.name}-%j.err",
            extra=spec.config.extra,
            mail_user=spec.config.mail_user or cfg.mail_user,
            mail_type=spec.config.mail_type or cfg.mail_type,
            signal=spec.config.signal or cfg.signal,
        )

    def _array_resources(
        self,
        run_dir: Path,
        spec: TaskSpec,
        array_value: str,
    ) -> JobResources:
        cfg = self.config
        return JobResources(
            job_name=f"{self.name}-{spec.name}",
            cpus=spec.config.cpus,
            time_limit=spec.config.time,
            mem=spec.config.mem,
            partition=spec.config.partition,
            account=spec.config.account or cfg.executor_account,
            array=array_value,
            output_path=run_dir / "logs" / f"{spec.name}-%A_%a.out",
            error_path=run_dir / "logs" / f"{spec.name}-%A_%a.err",
            extra=spec.config.extra,
            mail_user=spec.config.mail_user or cfg.mail_user,
            mail_type=spec.config.mail_type or cfg.mail_type,
            signal=spec.config.signal or cfg.signal,
        )


# --- module-level helpers --------------------------------------------------


def _make_run_id(workflow_name: str) -> str:
    """Generate a short, human-friendly run id.

    Format: ``<workflow>-<YYYYMMDD>-<4hex>``
    """
    from datetime import datetime, timezone

    date_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
    short = uuid.uuid4().hex[:4]
    return f"{workflow_name}-{date_str}-{short}"


def _resolve_executor(executor: Executor | str | None) -> Executor | None:
    if isinstance(executor, str):
        if executor == "local":
            from .executors.local import LocalExecutor

            return LocalExecutor()
        raise ValueError(f"Unknown executor shorthand {executor!r}.  Use 'local'.")
    return executor


def _resolve_index(explicit: int | None) -> int | None:
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
