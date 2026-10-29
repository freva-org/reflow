"""Exclusive resources: at most one active run per target.

A workflow declares which parameters name resources that two runs must
never use at the same time, typically an output location::

    wf = Workflow("era5-healpix", exclusive=["target"])

When a run starts, reflow *claims* the normalised values of those
parameters in the manifest store.  The check and the claim happen in one
transaction, so of two runs started at the same moment only one gets a
key.  Keys are global: a run of *another* workflow that claims the same
target conflicts too.  A claim is released when its run finishes,
fails or is cancelled.

If a run dies without finishing (its dispatch job was cancelled or
killed along with its tasks), its claim would block the target forever.
So before a conflict is reported, the holding run is checked with
`Executor.job_status`, which asks the live queue of any backend: if
neither its dispatch jobs nor its task jobs are queued or running, the
run is marked ``FAILED`` and its claims are released.  If the scheduler
cannot tell, the claim is respected and the conflict reported: resolve
it with ``cancel``.

Claims only protect runs that use the same manifest store.
"""

from __future__ import annotations

import getpass
import logging
import os
import re
import socket
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._types import RunState, TaskState
from .executors import Executor, JobStatus
from .stores import Store

logger = logging.getLogger(__name__)

#: What a workflow may pass as ``exclusive=``: parameter names, or a
#: function mapping the run parameters to resource keys.
ExclusiveSpec = Sequence[str] | Callable[[Mapping[str, Any]], Iterable[Any]]

_URL = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


class RunConflictError(RuntimeError):
    """A resource the new run needs is held by another active run."""

    def __init__(self, message: str, conflicts: Sequence[Conflict]) -> None:
        super().__init__(message)
        self.conflicts = tuple(conflicts)


@dataclass(frozen=True)
class Conflict:
    """One key held by another run."""

    key: str
    label: str
    holder: str
    detail: str


def normalise_key(value: Any) -> str:
    """Return the canonical form of a resource value.

    URLs (``s3://bucket/x/``) lose trailing slashes; anything else is
    treated as a local path and resolved, so ``./out`` and the absolute
    path of the same directory are the same key.
    """
    text = os.fspath(value) if isinstance(value, os.PathLike) else str(value)
    text = text.strip()
    if _URL.match(text):
        return text.rstrip("/")
    return str(Path(text).expanduser().resolve(strict=False))


def resource_keys(
    spec: ExclusiveSpec | None, parameters: Mapping[str, Any]
) -> dict[str, str]:
    """Map normalised keys to labels such as ``"target=/scratch/out"``.

    Raises
    ------
    ValueError
        If a named parameter is missing from *parameters*.

    """
    if spec is None:
        return {}
    keys: dict[str, str] = {}
    if callable(spec):
        for value in spec(parameters):
            key = normalise_key(value)
            keys[key] = key
        return keys
    for name in spec:
        if name not in parameters or parameters[name] is None:
            raise ValueError(
                f"exclusive parameter {name!r} has no value; it must be a "
                "parameter of the workflow"
            )
        value = parameters[name]
        values = value if isinstance(value, (list, tuple, set)) else [value]
        for item in values:
            key = normalise_key(item)
            keys[key] = f"{name}={key}"
    return keys


def owner_info(local: bool) -> dict[str, Any]:
    """Who holds a claim; host and pid only matter for local runs."""
    return {
        "user": getpass.getuser(),
        "host": socket.gethostname() if local else "",
        "pid": os.getpid() if local else 0,
    }


# --- liveness ----------------------------------------------------------------


@dataclass(frozen=True)
class Liveness:
    """Whether a holding run may still be working.

    ``alive`` is ``True`` (jobs queued or running), ``False`` (nothing
    can run any more) or ``None`` (cannot tell).
    """

    alive: bool | None
    detail: str


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def run_liveness(
    run_id: str, store: Store, executor: Executor, claim: Mapping[str, Any]
) -> Liveness:
    """Check whether *run_id* (holding *claim*) is still active.

    Only reads: neither the run nor its instances are changed, because
    the holder may belong to another user whose result files cannot be
    read from here.
    """
    run = store.get_run(run_id)
    if run is None or run.get("status") != RunState.RUNNING.value:
        return Liveness(False, "the run is no longer running")

    pid = int(claim.get("pid") or 0)
    if pid:
        host = str(claim.get("host") or "")
        if host != socket.gethostname():
            return Liveness(
                None, f"it runs locally on host {host} (pid {pid}); cannot check"
            )
        if _pid_alive(pid):
            return Liveness(True, f"it runs locally here (pid {pid})")
        return Liveness(False, f"its local process {pid} is gone")

    # A running run always has something in the scheduler: its newest
    # dispatch job waits (afterany) for the task jobs of the last pass,
    # and task jobs may outlive a cancelled dispatch job.  `job_status`
    # asks the live queue, so this works on every backend.
    dispatch = list(reversed(store.list_jobs(run_id, "dispatch")))
    tasks = list(
        dict.fromkeys(
            str(inst["job_id"])
            for inst in store.list_task_instances(
                run_id, states=[TaskState.SUBMITTED, TaskState.RUNNING]
            )
            if inst.get("job_id")
        )
    )
    if not dispatch and not tasks:
        return Liveness(None, "no scheduler jobs are recorded for it")
    unknown = False
    for what, job_ids in (
        ("a dispatch pass is queued or running", dispatch),
        ("task jobs are queued or running", tasks),
    ):
        if not job_ids:
            continue
        state = executor.jobs_status(job_ids)
        if state.active:
            return Liveness(True, what)
        unknown = unknown or state.status is JobStatus.UNKNOWN
    if unknown:
        return Liveness(
            None, "the scheduler cannot tell whether its jobs are still running"
        )
    return Liveness(False, "none of its jobs is queued or running any more")


# --- claiming ----------------------------------------------------------------


def claim(
    store: Store,
    run_id: str,
    keys: Mapping[str, str],
    executor: Executor,
    *,
    workflow: str,
    local: bool = False,
) -> None:
    """Claim *keys* for *run_id*, freeing claims of dead runs on the way.

    Raises
    ------
    RunConflictError
        If a key is held by a run that is (or may be) still active.

    """
    if not keys:
        return
    owner = owner_info(local)
    # Each round either succeeds, reports live holders, or frees at least
    # one dead holder; a few rounds cover holders freed concurrently.
    for _ in range(5):
        held = store.claim_resources(run_id, dict(keys), owner)
        if not held:
            return
        conflicts: list[Conflict] = []
        freed = False
        for key, row in held.items():
            holder = str(row["run_id"])
            liveness = run_liveness(holder, store, executor, row)
            if liveness.alive is False:
                logger.warning(
                    "Run %s held %s but is no longer active (%s); releasing it",
                    holder,
                    keys[key],
                    liveness.detail,
                )
                run = store.get_run(holder)
                if run is not None and run.get("status") == RunState.RUNNING.value:
                    store.update_run_status(holder, RunState.FAILED)
                store.release_resources(holder)
                freed = True
                continue
            conflicts.append(Conflict(key, keys[key], holder, liveness.detail))
        if conflicts:
            raise RunConflictError(
                _conflict_message(workflow, conflicts, store), conflicts
            )
        if not freed:
            break
    raise RunConflictError(
        f"Could not claim {', '.join(keys.values())} for a new run of "
        f"{workflow!r}: other runs keep claiming it.",
        [],
    )


def _conflict_message(
    workflow: str, conflicts: Sequence[Conflict], store: Store
) -> str:
    lines = [f"Cannot start a new run of {workflow!r}:"]
    for conflict in conflicts:
        run = store.get_run(conflict.holder) or {}
        who = run.get("user_id") or "?"
        created = str(run.get("created_at") or "?")[:16]
        lines.append(
            f"  {conflict.label} is held by run {conflict.holder} "
            f"(user {who}, created {created}): {conflict.detail}."
        )
    lines.append(
        "Wait for that run to finish, or cancel it with "
        "`cancel <run-id>` if it should not continue."
    )
    return "\n".join(lines)


__all__ = [
    "Conflict",
    "ExclusiveSpec",
    "Liveness",
    "RunConflictError",
    "claim",
    "normalise_key",
    "resource_keys",
    "run_liveness",
]
