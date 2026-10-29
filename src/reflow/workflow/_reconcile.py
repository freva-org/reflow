"""Find task instances whose scheduler job ended without a result.

A worker reports its outcome through a result file.  A worker that is
killed hard (out of memory, time limit followed by ``SIGKILL``, node
failure, ``scancel``) never writes one, so its instance would stay
``SUBMITTED`` or ``RUNNING`` forever: nothing downstream becomes ready,
the run is never finalised, and ``retry`` does not pick it up because
the instance is not ``FAILED``.

Two places close that gap, and neither needs the scheduler's
accounting:

`reconcile_awaited`
    Every follow-up dispatch job is submitted with an "after any"
    dependency on the jobs of its pass (Slurm/PBS/Flux ``afterany``,
    LSF ``ended()``, SGE ``-hold_jid``).  When it runs, the scheduler
    has therefore *guaranteed* that those jobs ended, whatever their
    outcome.  The awaited ids are stored under a reference that the
    dispatch job gets as ``--after-ref``; instances of those jobs that
    still have no result are lost.
`reconcile_ended`
    For ``retry``, which runs outside that chain: asks
    `Executor.job_status` (the live queue) whether each job has ended.

The scheduler's own reason (``OUT_OF_MEMORY``, ``TIMEOUT``, ...) is
added to the error text where the backend can tell it (`job_states`);
it is never needed for the decision.
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from .._types import TaskState
from ..executors import ElementState, Executor, JobStates, JobStatus
from ..stores import Store

logger = logging.getLogger(__name__)

#: Seconds to wait before declaring results missing.  Result files are
#: written on compute nodes, usually to a network file system, and may
#: take a moment to become visible where the dispatcher runs.
DEFAULT_RESULT_GRACE = 10.0


def result_grace() -> float:
    """Grace period in seconds; ``REFLOW_RESULT_GRACE`` overrides it."""
    try:
        return max(0.0, float(os.environ["REFLOW_RESULT_GRACE"]))
    except (KeyError, ValueError):
        return DEFAULT_RESULT_GRACE


def _waiting(
    store: Store, run_id: str, job_ids: Iterable[str] | None = None
) -> list[dict[str, Any]]:
    """Instances without a result yet, optionally only of *job_ids*."""
    wanted = None if job_ids is None else set(job_ids)
    return [
        inst
        for inst in store.list_task_instances(
            run_id, states=[TaskState.SUBMITTED, TaskState.RUNNING]
        )
        if inst.get("job_id") and (wanted is None or str(inst["job_id"]) in wanted)
    ]


def _states(executor: Executor, job_ids: Iterable[str]) -> dict[str, JobStates]:
    """Best-effort `job_states` per job, for reasons only."""
    states: dict[str, JobStates] = {}
    for job_id in job_ids:
        try:
            states[job_id] = executor.job_states(job_id)
        except Exception:  # noqa: BLE001 - the reason is optional
            logger.debug("job_states(%s) failed", job_id, exc_info=True)
            states[job_id] = JobStates()
    return states


def _message(inst: dict[str, Any], reason: str) -> str:
    job = str(inst["job_id"])
    index = inst.get("array_index")
    where = f"{job}_{index}" if index is not None and "," not in job else job
    if reason == "COMPLETED":
        return (
            f"Job {where} completed but wrote no result file, so the task's "
            "return value is lost.  The worker probably exited without "
            "finishing normally; see the job's log in <run_dir>/logs."
        )
    return (
        f"Job {where} ended without writing a result "
        f"(scheduler state: {reason or 'unknown'}).  The worker was "
        "probably killed (out of memory, time limit, node failure or "
        "cancellation); see the job's log in <run_dir>/logs."
    )


def _fail_lost(
    run_id: str,
    store: Store,
    executor: Executor,
    lost: list[dict[str, Any]],
    grace: float | None,
) -> int:
    """Wait for late results, then fail what is still without one."""
    wait = result_grace() if grace is None else grace
    if wait > 0:
        from ..results import ingest_results

        logger.info(
            "%d instance(s) of %s have no result although their job ended; "
            "checking again in %.0fs",
            len(lost),
            run_id,
            wait,
        )
        time.sleep(wait)
        ingest_results(run_id, store)
        still = {int(inst["id"]) for inst in _waiting(store, run_id)}
        lost = [inst for inst in lost if int(inst["id"]) in still]
    if not lost:
        return 0

    states = _states(executor, {str(inst["job_id"]) for inst in lost})
    by_reason: dict[str, int] = defaultdict(int)
    failed = 0
    for inst in lost:
        state: ElementState = states[str(inst["job_id"])].get(inst.get("array_index"))
        if state.active:
            # The scheduler says otherwise (e.g. a requeued job): trust it.
            logger.info(
                "Job %s is %s again; not failing its instance",
                inst["job_id"],
                state.reason or state.status.value,
            )
            continue
        store.update_task_failed(int(inst["id"]), _message(inst, state.reason))
        by_reason[state.reason or "unknown"] += 1
        failed += 1
    if failed:
        logger.warning(
            "Marked %d instance(s) of %s as FAILED: their job ended without a "
            "result (%s)",
            failed,
            run_id,
            ", ".join(f"{n}x {r}" for r, n in sorted(by_reason.items())),
        )
    return failed


def reconcile_awaited(
    run_id: str,
    store: Store,
    executor: Executor,
    after_ref: str,
    *,
    grace: float | None = None,
) -> int:
    """Fail instances of the jobs a dispatch pass waited for.

    Parameters
    ----------
    run_id : str
        Run to check.
    store : Store
        Manifest store; result files must have been ingested already.
    executor : Executor
        Only asked for the reason text (`Executor.job_states`).
    after_ref : str
        Reference under which the awaited job ids were stored
        (``kind="awaited"``).  The scheduler started this dispatch pass
        only after all of them ended.
    grace : float or None
        Seconds to wait for late result files before failing instances;
        see `result_grace`.  ``0`` disables the wait.

    Returns
    -------
    int
        Number of instances marked ``FAILED``.

    """
    awaited = store.list_jobs(run_id, "awaited", ref=after_ref)
    lost = _waiting(store, run_id, awaited)
    return _fail_lost(run_id, store, executor, lost, grace) if lost else 0


def reconcile_ended(
    run_id: str,
    store: Store,
    executor: Executor,
    *,
    task_name: str | None = None,
    grace: float | None = None,
) -> int:
    """Fail instances whose job the live queue reports as ended.

    Used by ``retry``, which does not run behind a dependency.  Jobs
    whose status is ``UNKNOWN`` (the query failed) are left alone.
    Returns the number of instances marked ``FAILED``.
    """
    waiting = [
        inst
        for inst in _waiting(store, run_id)
        if task_name is None or inst.get("task_name") == task_name
    ]
    status = {
        job_id: executor.job_status(job_id).status
        for job_id in {str(inst["job_id"]) for inst in waiting}
    }
    lost = [inst for inst in waiting if status[str(inst["job_id"])] is JobStatus.ENDED]
    return _fail_lost(run_id, store, executor, lost, grace) if lost else 0


__all__ = [
    "DEFAULT_RESULT_GRACE",
    "reconcile_awaited",
    "reconcile_ended",
    "result_grace",
]
