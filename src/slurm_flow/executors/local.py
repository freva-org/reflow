"""Local subprocess executor for testing without a workload manager."""

from __future__ import annotations

import logging
import subprocess
import uuid
from typing import Dict, List, Optional

from . import Executor, JobResources

logger = logging.getLogger(__name__)


class LocalExecutor(Executor):
    """Run tasks as local subprocesses.

    This executor is intended for development and integration testing.
    Jobs are launched synchronously via :func:`subprocess.run`.

    Parameters
    ----------
    capture_output : bool
        Whether to capture stdout/stderr of child processes.

    Examples
    --------
    >>> executor = LocalExecutor()
    >>> job_id = executor.submit(resources, ["python", "flow.py", "worker", ...])
    """

    def __init__(self, capture_output: bool = False) -> None:
        self.capture_output = capture_output
        self._procs: Dict[str, Optional[subprocess.CompletedProcess[str]]] = {}

    def submit(
        self,
        resources: JobResources,
        command: List[str],
    ) -> str:
        """Run a command synchronously as a local subprocess.

        Parameters
        ----------
        resources : JobResources
            Ignored for local execution (logged for reference).
        command : List[str]
            Command to execute.

        Returns
        -------
        str
            Synthetic job identifier.
        """
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
            if proc.returncode != 0:
                logger.warning(
                    "LOCAL [%s] exited with code %d", job_id, proc.returncode
                )
        except Exception:
            logger.exception("LOCAL [%s] failed to start", job_id)
            self._procs[job_id] = None
        return job_id

    def cancel(self, job_id: str) -> None:
        """No-op for local executor (process already completed).

        Parameters
        ----------
        job_id : str
            Job identifier.
        """
        logger.info("LOCAL cancel is a no-op for %s", job_id)

    def job_state(self, job_id: str) -> Optional[str]:
        """Return the exit status of a completed local job.

        Parameters
        ----------
        job_id : str
            Job identifier.

        Returns
        -------
        str or None
            ``"COMPLETED"`` or ``"FAILED"``, or ``None`` if unknown.
        """
        proc = self._procs.get(job_id)
        if proc is None:
            return None
        return "COMPLETED" if proc.returncode == 0 else "FAILED"
