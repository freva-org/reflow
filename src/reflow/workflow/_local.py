"""In-process local runner for the Workflow class.

Executes all tasks in topological order without any subprocess or
scheduler overhead.  Array tasks are expanded inline and optionally
run in parallel using :mod:`concurrent.futures`.

Mixed into :class:`~reflow.workflow.Workflow` via multiple inheritance.
"""

from __future__ import annotations

import getpass
import logging
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .._types import RunState, TaskState
from ..cache import (
    compute_identity,
    compute_input_hash,
    compute_output_hash,
    verify_cached_output,
)
from ..params import WireMode, extract_base_type, infer_wire_mode
from ..stores import Store
from ..stores.sqlite import SqliteStore
from ._helpers import build_kwargs, make_run_id

if TYPE_CHECKING:
    from ..flow import TaskSpec
    from ..run import Run

logger = logging.getLogger(__name__)


def _run_task_func(func_module: str, func_qualname: str, kwargs: dict[str, Any]) -> Any:
    """Picklable wrapper that imports and calls a task function.

    Used by ProcessPoolExecutor so that array elements can run in
    parallel worker processes.
    """
    import importlib

    parts = func_qualname.split(".")
    module = importlib.import_module(func_module)
    obj: Any = module
    for part in parts:
        obj = getattr(obj, part)
    return obj(**kwargs)


class LocalRunMixin:
    """In-process execution of the full workflow DAG.

    All methods reference ``self.tasks``, ``self.config``, etc. which
    are provided by the concrete :class:`Workflow` class through MRO.
    """

    def run_local(
        self,
        run_dir: str | Path,
        *,
        store: Store | None = None,
        max_workers: int = 1,
        force: bool = False,
        force_tasks: list[str] | None = None,
        verify: bool = False,
        on_error: str = "stop",
        **parameters: Any,
    ) -> Run:
        """Execute the entire workflow locally, in-process.

        Tasks run in topological order.  Array tasks are expanded and
        their elements optionally parallelised across *max_workers*
        processes.  No subprocess dispatch or workload manager is
        involved.

        Parameters
        ----------
        run_dir : str or Path
            Shared working directory.
        store : Store or None
            Manifest store.  Defaults to the shared SQLite store.
        max_workers : int
            Maximum parallel processes for array element execution.
            ``1`` (default) means fully sequential.
        force : bool
            Skip the Merkle cache entirely.
        force_tasks : list[str] or None
            Task names to force-run even if cached.
        verify : bool
            Run output verification on cache hits.
        on_error : ``"stop"`` or ``"continue"``
            ``"stop"`` aborts the run on the first failure.
            ``"continue"`` marks the task as failed and keeps going
            (downstream dependants will be skipped).
        **parameters
            Run parameters (same as ``submit``).

        Returns
        -------
        Run
            An interactive :class:`~reflow.Run` handle.

        Raises
        ------
        RuntimeError
            If *on_error* is ``"stop"`` and any task fails.

        """
        from ..run import Run

        self.validate()  # type: ignore[attr-defined]
        self._check_submit_params(parameters)  # type: ignore[attr-defined]

        rd = Path(run_dir).expanduser().resolve()
        rd.mkdir(parents=True, exist_ok=True)
        (rd / "logs").mkdir(parents=True, exist_ok=True)

        config = self.config  # type: ignore[attr-defined]
        st = store or SqliteStore.default(config)
        st.init()

        run_id = make_run_id(self.name)  # type: ignore[attr-defined]
        user_id = getpass.getuser()

        params: dict[str, Any] = dict(parameters)
        params["run_dir"] = str(rd)
        if force:
            params["__force__"] = True
        if force_tasks:
            params["__force_tasks__"] = force_tasks

        st.insert_run(run_id, self.name, user_id, params)  # type: ignore[attr-defined]

        for spec in self.tasks.values():  # type: ignore[attr-defined]
            st.insert_task_spec(
                run_id,
                spec.name,
                spec.config.array,
                asdict(spec.config),
                self._effective_dependencies(spec),  # type: ignore[attr-defined]
            )

        topo = self._topological_order()  # type: ignore[attr-defined]
        failed_tasks: set[str] = set()

        for task_name in topo:
            spec: TaskSpec = self.tasks[task_name]  # type: ignore
            deps = set(self._effective_dependencies(spec))  # type: ignore[attr-defined]
            if deps & failed_tasks:
                logger.warning(
                    "Skipping %s — upstream dependency failed.",
                    task_name,
                )
                failed_tasks.add(task_name)
                continue

            if spec.config.array:
                ok = self._local_run_array(
                    run_id,
                    rd,
                    spec,
                    st,
                    params,
                    max_workers=max_workers,
                    force=force,
                    force_tasks=force_tasks or [],
                    verify=verify,
                )
            else:
                ok = self._local_run_single(
                    run_id,
                    rd,
                    spec,
                    st,
                    params,
                    force=force,
                    force_tasks=force_tasks or [],
                    verify=verify,
                )

            if not ok:
                failed_tasks.add(task_name)
                if on_error == "stop":
                    st.update_run_status(run_id, RunState.FAILED)
                    raise RuntimeError(
                        f"Task {task_name!r} failed.  Run {run_id} aborted."
                    )

        if failed_tasks:
            st.update_run_status(run_id, RunState.FAILED)
        else:
            st.update_run_status(run_id, RunState.SUCCESS)

        logger.info(
            "Local run %s finished (%s).",
            run_id,
            "FAILED" if failed_tasks else "SUCCESS",
        )
        return Run(workflow=self, run_id=run_id, run_dir=rd, store=st)  # type: ignore[arg-type]

    # --- internals ---------------------------------------------------------

    def _local_should_force(
        self,
        task_name: str,
        force: bool,
        force_tasks: list[str],
    ) -> bool:
        return force or task_name in force_tasks

    def _local_resolve_inputs(
        self,
        store: Store,
        run_id: str,
        spec: TaskSpec,
    ) -> dict[str, Any]:
        """Resolve Result-wired inputs from the store.

        Identical logic to DispatchMixin._resolve_result_inputs but
        kept separate so the local runner doesn't depend on the
        dispatch mixin's internal API.
        """
        from typing import get_type_hints

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
                __import__("inspect").Parameter.empty,
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

    def _local_try_cache(
        self,
        store: Store,
        run_id: str,
        spec: TaskSpec,
        direct_inputs: dict[str, Any],
        array_index: int | None,
        upstream_output_hashes: list[str],
        force: bool,
        force_tasks: list[str],
        verify: bool,
    ) -> bool:
        """Attempt cache resolution (mirrors DispatchMixin._try_cache)."""
        if not spec.config.cache:
            return False
        if self._local_should_force(spec.name, force, force_tasks):
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

        if verify and not verify_cached_output(
            cached["output"],
            spec.return_type,
            spec.verify,
        ):
            logger.info("Cache stale for %s (identity=%s)", spec.name, identity)
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

    def _local_collect_upstream_hashes(
        self,
        store: Store,
        run_id: str,
        spec: TaskSpec,
    ) -> list[str]:
        hashes: list[str] = []
        for dep in self._effective_dependencies(spec):  # type: ignore[attr-defined]
            hashes.extend(store.get_all_output_hashes(run_id, dep))
        return sorted(hashes)

    # --- single task -------------------------------------------------------

    def _local_run_single(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        store: Store,
        run_params: dict[str, Any],
        force: bool,
        force_tasks: list[str],
        verify: bool,
    ) -> bool:
        """Run one singleton task in-process.  Returns True on success."""
        result_inputs = self._local_resolve_inputs(store, run_id, spec)
        up_hashes = self._local_collect_upstream_hashes(store, run_id, spec)

        if self._local_try_cache(
            store,
            run_id,
            spec,
            result_inputs,
            None,
            up_hashes,
            force,
            force_tasks,
            verify,
        ):
            return True

        input_hash = compute_input_hash(
            spec.name,
            spec.config.version,
            result_inputs,
        )
        identity = compute_identity(input_hash, up_hashes)
        iid = store.insert_task_instance(
            run_id,
            spec.name,
            None,
            TaskState.RUNNING,
            result_inputs,
            identity=identity,
            input_hash=input_hash,
        )

        kwargs = build_kwargs(spec, run_params, result_inputs)

        try:
            output = spec.func(**kwargs)
            out_hash = compute_output_hash(output)
            store.update_task_success(iid, output, output_hash=out_hash)
            logger.info("Task %s completed.", spec.name)
            return True
        except Exception:
            store.update_task_failed(iid, traceback.format_exc())
            logger.error("Task %s failed:\n%s", spec.name, traceback.format_exc())
            return False

    # --- array task --------------------------------------------------------

    def _local_find_fan_out_param(self, spec: TaskSpec) -> str | None:
        """Mirrors DispatchMixin._find_fan_out_param."""
        from typing import get_type_hints

        try:
            hints = get_type_hints(spec.func, include_extras=True)
        except Exception:
            return None

        for pname, result in spec.result_deps.items():
            if result.broadcast:
                continue
            raw_ann = hints.get(pname)
            if raw_ann is None:
                continue
            param_type = extract_base_type(raw_ann)
            first_up = self.tasks.get(result.steps[0])  # type: ignore[attr-defined]
            if first_up is None or first_up.return_type in (
                None,
                __import__("inspect").Parameter.empty,
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

        for pname, result in spec.result_deps.items():
            if not result.broadcast:
                return pname
        return None

    def _local_run_array(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        store: Store,
        run_params: dict[str, Any],
        max_workers: int,
        force: bool,
        force_tasks: list[str],
        verify: bool,
    ) -> bool:
        """Run an array task in-process.  Returns True if all elements succeed."""
        result_inputs = self._local_resolve_inputs(store, run_id, spec)
        if not result_inputs:
            logger.warning("No result inputs for array task %s", spec.name)
            return False

        fan_param = self._local_find_fan_out_param(spec)
        if fan_param is None or fan_param not in result_inputs:
            logger.warning("No fan-out parameter for array task %s", spec.name)
            return False

        fan_items = result_inputs[fan_param]
        if not isinstance(fan_items, list):
            fan_items = [fan_items]
        if not fan_items:
            logger.warning("Empty fan-out list for array task %s", spec.name)
            return False

        broadcast_params: set[str] = {
            pname for pname, res in spec.result_deps.items() if res.broadcast
        }

        up_hashes = self._local_collect_upstream_hashes(store, run_id, spec)

        # Build per-element payloads.
        pending: list[tuple[int, dict[str, Any]]] = []
        for idx, element in enumerate(fan_items):
            payload: dict[str, Any] = {fan_param: element}
            for pname, value in result_inputs.items():
                if pname == fan_param:
                    continue
                if pname in broadcast_params:
                    payload[pname] = value
                elif isinstance(value, list) and len(value) == len(fan_items):
                    payload[pname] = value[idx]
                else:
                    payload[pname] = value

            if self._local_try_cache(
                store,
                run_id,
                spec,
                payload,
                idx,
                up_hashes,
                force,
                force_tasks,
                verify,
            ):
                continue
            pending.append((idx, payload))

        if not pending:
            logger.info(
                "All %d instances of %s resolved from cache.",
                len(fan_items),
                spec.name,
            )
            return True

        all_ok = True
        if max_workers <= 1:
            # Sequential execution — simple and debuggable.
            for idx, payload in pending:
                ok = self._local_run_one_element(
                    run_id,
                    spec,
                    store,
                    run_params,
                    payload,
                    idx,
                    up_hashes,
                )
                if not ok:
                    all_ok = False
        else:
            # Parallel execution via ProcessPoolExecutor.
            all_ok = self._local_run_elements_parallel(
                run_id,
                run_dir,
                spec,
                store,
                run_params,
                pending,
                up_hashes,
                max_workers,
            )

        return all_ok

    def _local_run_one_element(
        self,
        run_id: str,
        spec: TaskSpec,
        store: Store,
        run_params: dict[str, Any],
        payload: dict[str, Any],
        idx: int,
        up_hashes: list[str],
    ) -> bool:
        """Execute a single array element in-process."""
        input_hash = compute_input_hash(
            spec.name,
            spec.config.version,
            payload,
        )
        identity = compute_identity(input_hash, up_hashes)
        iid = store.insert_task_instance(
            run_id,
            spec.name,
            idx,
            TaskState.RUNNING,
            payload,
            identity=identity,
            input_hash=input_hash,
        )
        kwargs = build_kwargs(spec, run_params, payload)
        try:
            output = spec.func(**kwargs)
            out_hash = compute_output_hash(output)
            store.update_task_success(iid, output, output_hash=out_hash)
            logger.debug("Task %s[%d] completed.", spec.name, idx)
            return True
        except Exception:
            store.update_task_failed(iid, traceback.format_exc())
            logger.error(
                "Task %s[%d] failed:\n%s",
                spec.name,
                idx,
                traceback.format_exc(),
            )
            return False

    def _local_run_elements_parallel(
        self,
        run_id: str,
        run_dir: Path,
        spec: TaskSpec,
        store: Store,
        run_params: dict[str, Any],
        pending: list[tuple[int, dict[str, Any]]],
        up_hashes: list[str],
        max_workers: int,
    ) -> bool:
        """Execute array elements in parallel processes.

        Falls back to sequential if the task function is not
        picklable (e.g. closures or lambdas).
        """
        # Pre-register instances as PENDING so we can track them.
        instance_ids: dict[int, int] = {}
        for idx, payload in pending:
            input_hash = compute_input_hash(
                spec.name,
                spec.config.version,
                payload,
            )
            identity = compute_identity(input_hash, up_hashes)
            iid = store.insert_task_instance(
                run_id,
                spec.name,
                idx,
                TaskState.RUNNING,
                payload,
                identity=identity,
                input_hash=input_hash,
            )
            instance_ids[idx] = iid

        effective_workers = min(max_workers, len(pending))
        if spec.config.array_parallelism is not None:
            effective_workers = min(effective_workers, spec.config.array_parallelism)

        all_ok = True
        try:
            with ProcessPoolExecutor(max_workers=effective_workers) as pool:
                futures = {}
                for idx, payload in pending:
                    kwargs = build_kwargs(spec, run_params, payload)
                    future = pool.submit(
                        _run_task_func,
                        spec.func.__module__,
                        spec.func.__qualname__,
                        kwargs,
                    )
                    futures[future] = idx

                for future in as_completed(futures):
                    idx = futures[future]
                    iid = instance_ids[idx]
                    try:
                        output = future.result()
                        out_hash = compute_output_hash(output)
                        store.update_task_success(iid, output, output_hash=out_hash)
                        logger.debug("Task %s[%d] completed.", spec.name, idx)
                    except Exception:
                        store.update_task_failed(iid, traceback.format_exc())
                        logger.error(
                            "Task %s[%d] failed:\n%s",
                            spec.name,
                            idx,
                            traceback.format_exc(),
                        )
                        all_ok = False
        except (AttributeError, TypeError) as exc:
            # Function not picklable — fall back to sequential.
            logger.warning(
                "Parallel execution failed for %s (%s), falling back to sequential.",
                spec.name,
                exc,
            )
            for idx, payload in pending:
                iid = instance_ids[idx]
                kwargs = build_kwargs(spec, run_params, payload)
                try:
                    output = spec.func(**kwargs)
                    out_hash = compute_output_hash(output)
                    store.update_task_success(iid, output, output_hash=out_hash)
                except Exception:
                    store.update_task_failed(iid, traceback.format_exc())
                    all_ok = False

        return all_ok
