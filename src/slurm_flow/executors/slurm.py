"""Slurm executor for slurm_flow."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
from typing import List, Optional

from . import Executor, JobResources

logger = logging.getLogger(__name__)


class SlurmExecutor(Executor):
    """Submit, cancel, and query Slurm jobs.

    Parameters
    ----------
    mode : str
        ``"sbatch"`` for real submission, ``"dry-run"`` to print commands only.
    sbatch : str
        Path to the ``sbatch`` binary.
    scancel : str
        Path to the ``scancel`` binary.
    sacct : str
        Path to the ``sacct`` binary.
    python : str
        Python interpreter used inside ``sbatch --wrap``.

    Examples
    --------
    >>> executor = SlurmExecutor.from_environment()
    >>> job_id = executor.submit(resources, ["python", "flow.py", "worker", ...])
    """

    def __init__(
        self,
        mode: str = "sbatch",
        sbatch: str = "sbatch",
        scancel: str = "scancel",
        sacct: str = "sacct",
        python: str = "",
    ) -> None:
        self.mode = mode
        self.sbatch = sbatch
        self.scancel = scancel
        self.sacct = sacct
        self.python = python or sys.executable

    @classmethod
    def from_environment(cls) -> "SlurmExecutor":
        """Build an executor from ``SLURM_FLOW_*`` environment variables.

        Recognised variables:

        - ``SLURM_FLOW_MODE`` – ``"sbatch"`` (default) or ``"dry-run"``
        - ``SLURM_FLOW_SBATCH`` – path to ``sbatch``
        - ``SLURM_FLOW_SCANCEL`` – path to ``scancel``
        - ``SLURM_FLOW_SACCT`` – path to ``sacct``
        - ``SLURM_FLOW_PYTHON`` – Python interpreter for workers

        Returns
        -------
        SlurmExecutor
        """
        return cls(
            mode=os.getenv("SLURM_FLOW_MODE", "sbatch").strip().lower(),
            sbatch=os.getenv("SLURM_FLOW_SBATCH", "sbatch"),
            scancel=os.getenv("SLURM_FLOW_SCANCEL", "scancel"),
            sacct=os.getenv("SLURM_FLOW_SACCT", "sacct"),
            python=os.getenv("SLURM_FLOW_PYTHON", sys.executable),
        )

    # -- Executor interface ------------------------------------------------

    def submit(
        self,
        resources: JobResources,
        command: List[str],
    ) -> str:
        """Submit a Slurm job via ``sbatch --wrap``.

        Parameters
        ----------
        resources : JobResources
            Requested resources.
        command : List[str]
            The command to wrap.

        Returns
        -------
        str
            Slurm job id, or ``"DRYRUN"`` in dry-run mode.
        """
        cmd = self._build_sbatch(resources, command)
        if self.mode == "dry-run":
            logger.info("DRY-RUN: %s", " ".join(cmd))
            return "DRYRUN"
        logger.debug("sbatch: %s", " ".join(cmd))
        output = subprocess.check_output(cmd, text=True).strip()
        logger.info("Submitted job %s", output)
        return output

    def cancel(self, job_id: str) -> None:
        """Cancel a Slurm job.

        Parameters
        ----------
        job_id : str
            Slurm job id.
        """
        if job_id == "DRYRUN":
            logger.info("DRY-RUN: scancel skipped for DRYRUN job")
            return
        cmd = [self.scancel, job_id]
        logger.info("scancel %s", job_id)
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as exc:
            logger.warning("scancel %s returned %d", job_id, exc.returncode)

    def job_state(self, job_id: str) -> Optional[str]:
        """Query Slurm job state via ``sacct``.

        Parameters
        ----------
        job_id : str
            Slurm job id.

        Returns
        -------
        str or None
            Slurm state string (e.g. ``"COMPLETED"``, ``"RUNNING"``),
            or ``None`` if the job cannot be found.
        """
        if job_id == "DRYRUN":
            return None
        try:
            output = subprocess.check_output(
                [
                    self.sacct,
                    "-j",
                    job_id,
                    "--noheader",
                    "--parsable2",
                    "--format=State",
                ],
                text=True,
            ).strip()
        except subprocess.CalledProcessError:
            return None
        lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
        return lines[0] if lines else None

    # -- internal helpers --------------------------------------------------

    def _build_sbatch(
        self,
        resources: JobResources,
        command: List[str],
    ) -> List[str]:
        """Assemble the full ``sbatch`` command line.

        Parameters
        ----------
        resources : JobResources
            Requested resources.
        command : List[str]
            Command to wrap.

        Returns
        -------
        List[str]
            ``sbatch`` invocation.
        """
        parts: List[str] = [
            self.sbatch,
            "--parsable",
            "--job-name", resources.job_name,
            "--cpus-per-task", str(resources.cpus),
            "--time", resources.time_limit,
            "--mem", resources.mem,
            "--partition", resources.partition,
        ]
        if resources.account:
            parts.extend(["-A", resources.account])
        if resources.array:
            parts.extend(["--array", resources.array])
        if resources.output_path is not None:
            parts.extend(["--output", str(resources.output_path)])
        if resources.error_path is not None:
            parts.extend(["--error", str(resources.error_path)])

        # pass through arbitrary extras
        for key, value in resources.extra.items():
            flag = f"--{key}" if not key.startswith("-") else key
            parts.extend([flag, str(value)])

        parts.extend(["--wrap", shlex.join(command)])
        return parts
