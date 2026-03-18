"""Local subprocess executor for testing without a workload manager."""

from __future__ import annotations

import logging
import subprocess
import uuid

from . import Executor, JobResources

logger = logging.getLogger(__name__)


class LocalExecutor(Executor):
    """Run tasks as local subprocesses (synchronous)."""

    def __init__(self, capture_output: bool = False) -> None:
        self.capture_output = capture_output
        self._procs: dict[str, subprocess.CompletedProcess[str] | None] = {}

    def submit(self, resources: JobResources, command: list[str]) -> str:
        job_id = f"local-{uuid.uuid4().hex[:8]}"
        logger.info("LOCAL [%s]: %s", job_id, " ".join(command))
        try:
            proc = subprocess.run(
                command,
                text=True,
                capture_output=self.capture_output,
                check=False,
            )
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
