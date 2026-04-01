"""Dispatch mixin for the Workflow class.

Contains the dispatch loop, cache resolution, result wiring,
fan-out logic, and run finalisation.  Mixed into
:class:`~reflow.workflow.Workflow` via multiple inheritance.
"""

from __future__ import annotations

import inspect
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, get_type_hints

from .._types import RunState, TaskState
from ..cache import (
    compute_identity,
    compute_input_hash,
    compute_output_hash,
    verify_cached_output,
)
from ..executors import Executor
from ..flow import TaskSpec
from ..params import (
    WireMode,
    extract_base_type,
    infer_wire_mode,
)
from ..stores import Store
from ._helpers import default_executor

if TYPE_CHECKING:
    from ..config import Config

logger = logging.getLogger(__name__)


class DispatchMixin:
    """Methods that implement the dispatch cycle.

    These are separated from the core :class:`Workflow` to keep file
    sizes manageable.  All methods reference ``self.tasks``,
    ``self.config``, ``self._effective_dependencies``, etc. which are
    provided by the concrete :class:`Workflow` class through MRO.
    """

    # --- public entry point ------------------------------------------------

    def dispatch(
        self,
        run_id: str,
        store: Store,
        run_dir: Path,
        executor: Executor | None = None,
        verify: bool = False,
    ) -> None:
        """Inspect the manifest and submit runnable tasks.

        Before dispatching, ingests any worker result files from
        ``<run_dir>/results/`` into the store.  After submitting
        worker jobs, submits a single follow-up dispatch with a
        scheduler dependency on all submitted jobs.

        Parameters
        ----------
        run_id : str
            Run identifier.
        store : Store
            Manifest store for the run.
        run_dir : Path
            Working directory for the run.
        executor : Executor or None
            Explicit executor override.
        verify : bool
            If ``True``, verify cached outputs before accepting
            cache hits.

        """
        from ..results import ingest_results

        n = ingest_results(run_id, store)
        if n > 0:
            logger.info("Ingested %d worker result(s)", n)

        config: Config = self.config  # type: ignore[attr-defined]
        exc = executor or default_executor(config)

        submitted_job_ids: list[str] = []
        for task_name in self._topological_order():  # type: ignore[attr-defined]
            spec: TaskSpec = self.tasks[task_name]  # type: ignore[attr-defined]
            if spec.config.array:
                jid = self._dispatch_array(
                    run_id,
                    run_dir,
                    spec,
                    store,
                    exc,
                    verify=verify,
                )
            else:
                jid = self._dispatch_single(
                    run_id,
                    run_dir,
                    spec,
                    store,
                    exc,
                    verify=verify,
                )
            if jid is not None:
                submitted_job_ids.append(jid)

        self._maybe_finalise_run(run_id, store)

        if submitted_job_ids:
            dep_opts = exc.dependency_options(submitted_job_ids)
            self._submit_dispatch(  # type: ignore[attr-defined]
                exc,
                run_id,
                run_dir,
                store,
                dependency_options=dep_opts,
            )

    # --- dependency helpers ------------------------------------------------

    def _all_deps_satisfied(
        self,
        store: Store,
        run_id: str,
        spec: TaskSpec,
    ) -> bool:
        return all(
            store.dependency_is_satisfied(run_id, dep)
            for dep in self._effective_dependencies(spec)  # type: ignore[attr-defined]
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
                upstream: TaskSpec = self.tasks[step]  # type: ignore[attr-defined]
                if upstream.config.array:
                    all_values.extend(store.get_array_outputs(run_id, step))
                else:
                    output = store.get_singleton_output(run_id, step)
                    if output is not None:
                        all_values.append(output)

            first_up: TaskSpec = self.tasks[result.steps[0]]  # type: ignore[attr-defined]
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
                    broadcast=result.broadcast,
                )

            if wire == WireMode.DIRECT:
                resolved[pname] = all_values[0] if all_values else None
            elif wire == WireMode.BROADCAST:
                # Pass the whole upstream output as-is.
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
            # Broadcast params never determine the fan-out dimension.
            if result.broadcast:
                continue
            raw_ann = hints.get(pname)
            if raw_ann is None:
                continue
            param_type = extract_base_type(raw_ann)
            first_up = self.tasks.get(result.steps[0])  # type: ignore[attr-defined]
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
                    broadcast=result.broadcast,
                )
            except TypeError:
                continue
            if wire in (WireMode.FAN_OUT, WireMode.CHAIN_FLATTEN):
                return pname
        # Fall back to the first non-broadcast result dep.
        for pname, result in spec.result_deps.items():
            if not result.broadcast:
                return pname
        return None

    # --- cache -------------------------------------------------------------

    def _should_force(
        self,
        store: Store,
        run_id: str,
        task_name: str,
    ) -> bool:
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

        if verify:
            if not verify_cached_output(
                cached["output"],
                spec.return_type,
                spec.verify,
            ):
                logger.info(
                    "Cache stale for %s (identity=%s)",
                    spec.name,
                    identity,
                )
                return False

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
            "Cache hit for %s[%s] (identity=%s)",
            spec.name,
            array_index,
            identity,
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
        for dep_name in self._effective_dependencies(spec):  # type: ignore[attr-defined]
            dep_hashes = store.get_all_output_hashes(run_id, dep_name)
            hashes.extend(dep_hashes)
        return sorted(hashes)

    # --- single / array dispatch -------------------------------------------

    def _dispatch_single(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        store: Store,
        executor: Executor,
        verify: bool = False,
    ) -> str | None:
        """Dispatch a singleton task.  Returns the job ID or None."""
        if not self._all_deps_satisfied(store, run_id, spec):
            return None
        row = store.get_task_instance(run_id, spec.name, None)
        if row is None:
            payload = self._resolve_result_inputs(store, run_id, spec)
            up_hashes = self._collect_upstream_output_hashes(store, run_id, spec)

            if self._try_cache(
                store,
                run_id,
                spec,
                payload,
                None,
                up_hashes,
                verify=verify,
            ):
                return None

            input_hash = compute_input_hash(
                spec.name,
                spec.config.version,
                payload,
            )
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
            return None
        job_id = executor.submit(
            self._single_resources(run_dir, spec),  # type: ignore[attr-defined]
            self._worker_command(run_id, run_dir, spec.name, store),  # type: ignore[attr-defined]
        )
        store.update_task_submitted(run_id, spec.name, job_id)
        return job_id

    def _dispatch_array(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        store: Store,
        executor: Executor,
        verify: bool = False,
    ) -> str | None:
        """Dispatch an array task.  Returns the job ID or None."""
        if not self._all_deps_satisfied(store, run_id, spec):
            return None

        existing = store.list_task_instances(run_id, task_name=spec.name)
        retrying = [
            r for r in existing if TaskState(str(r["state"])) == TaskState.RETRYING
        ]
        if retrying:
            arr = ",".join(str(int(r["array_index"])) for r in retrying)
            job_id = executor.submit(
                self._array_resources(run_dir, spec, arr),  # type: ignore[attr-defined]
                self._worker_command(run_id, run_dir, spec.name, store),  # type: ignore[attr-defined]
            )
            store.update_task_submitted(run_id, spec.name, job_id)
            return job_id

        if store.count_task_instances(run_id, spec.name) > 0:
            return None

        result_inputs = self._resolve_result_inputs(store, run_id, spec)
        if not result_inputs:
            return None
        fan_param = self._find_fan_out_param(spec)
        if fan_param is None or fan_param not in result_inputs:
            return None
        fan_items = result_inputs[fan_param]
        if not isinstance(fan_items, list):
            fan_items = [fan_items]
        if not fan_items:
            return None

        # Identify broadcast params so we never index into them.
        broadcast_params: set[str] = {
            pname for pname, res in spec.result_deps.items() if res.broadcast
        }

        up_hashes = self._collect_upstream_output_hashes(store, run_id, spec)
        pending_indices: list[int] = []

        for idx, element in enumerate(fan_items):
            payload: dict[str, Any] = {fan_param: element}
            for pname, value in result_inputs.items():
                if pname == fan_param:
                    continue
                # Broadcast params always get the whole value.
                if pname in broadcast_params:
                    payload[pname] = value
                elif isinstance(value, list) and len(value) == len(fan_items):
                    payload[pname] = value[idx]
                else:
                    payload[pname] = value

            if self._try_cache(
                store,
                run_id,
                spec,
                payload,
                idx,
                up_hashes,
                verify=verify,
            ):
                continue

            input_hash = compute_input_hash(
                spec.name,
                spec.config.version,
                payload,
            )
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
            logger.info(
                "All %d instances of %s resolved from cache.",
                len(fan_items),
                spec.name,
            )
            return None

        if len(pending_indices) == len(fan_items):
            arr_str = f"0-{len(fan_items) - 1}"
        else:
            arr_str = ",".join(str(i) for i in pending_indices)
        if spec.config.array_parallelism is not None:
            arr_str = f"{arr_str}%{spec.config.array_parallelism}"
        job_id = executor.submit(
            self._array_resources(run_dir, spec, arr_str),  # type: ignore[attr-defined]
            self._worker_command(run_id, run_dir, spec.name, store),  # type: ignore[attr-defined]
        )
        store.update_task_submitted(run_id, spec.name, job_id)
        return job_id

    # --- finalisation ------------------------------------------------------

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
