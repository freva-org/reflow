"""Local subprocess executor for testing without a workload manager."""

from __future__ import annotations

import logging
import uuid

from . import ElementState, Executor, JobResources, JobStates, JobStatus
from .util import CommandResult, run_cmd

logger = logging.getLogger(__name__)


class LocalExecutor(Executor):
    """Run tasks as local subprocesses (synchronous)."""

    array_index_env_var: str = "REFLOW_LOCAL_ARRAY_INDEX"
    _internal_keys: frozenset[str] = frozenset({"python", "mode"})

    def __init__(self, capture_output: bool = False) -> None:
        super().__init__()
        self.capture_output = capture_output
        self._procs: dict[str, CommandResult | None] = {}

    def submit(self, resources: JobResources, command: list[str]) -> str:
        job_id = f"local-{uuid.uuid4().hex[:8]}"
        logger.info("LOCAL [%s]: %s", job_id, " ".join(command))
        try:
            proc = run_cmd(command, check=False)
            self._procs[job_id] = proc
        except Exception:
            logger.exception("LOCAL [%s] failed to start", job_id)
            self._procs[job_id] = None
        return job_id

    def cancel(self, job_id: str) -> None:
        logger.info("LOCAL cancel is a no-op for %s", job_id)

    def job_state(self, job_id: str) -> str | None:
        proc = self._procs.get(job_id)
        if proc is None:
            return None
        return "COMPLETED" if proc.returncode == 0 else "FAILED"

    def job_states(self, job_id: str) -> JobStates:
        """Local jobs run synchronously: once an id exists, the job has ended.

        `submit` only returns after the subprocess exited, so any process
        that sees a ``local-…`` id (another dispatch pass, a status call)
        sees a finished job.  The exit status is known only in the
        process that ran it.
        """
        proc = self._procs.get(job_id)
        reason = ""
        if proc is not None:
            reason = "COMPLETED" if proc.returncode == 0 else "FAILED"
        return JobStates(default=ElementState(JobStatus.ENDED, reason))
