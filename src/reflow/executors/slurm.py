"""Slurm executor for reflow."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys

from . import Executor, JobResources

logger = logging.getLogger(__name__)


class SlurmExecutor(Executor):
    """Submit, cancel, and query Slurm jobs.

    Parameters
    ----------
    mode : str
        ``"sbatch"`` for real submission, ``"dry-run"`` to print only.
    sbatch : str
        Path to the ``sbatch`` binary.
    scancel : str
        Path to the ``scancel`` binary.
    sacct : str
        Path to the ``sacct`` binary.
    python : str
        Python interpreter used inside ``sbatch --wrap``.

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
    def from_environment(cls) -> SlurmExecutor:
        """Build from ``REFLOW_*`` environment variables.

        Recognised variables:

        - ``REFLOW_MODE`` -- ``"sbatch"`` (default) or ``"dry-run"``
        - ``REFLOW_SBATCH`` -- path to ``sbatch``
        - ``REFLOW_SCANCEL`` -- path to ``scancel``
        - ``REFLOW_SACCT`` -- path to ``sacct``
        - ``REFLOW_PYTHON`` -- Python interpreter for workers
        """
        return cls(
            mode=os.getenv("REFLOW_MODE", "sbatch").strip().lower(),
            sbatch=os.getenv("REFLOW_SBATCH", "sbatch"),
            scancel=os.getenv("REFLOW_SCANCEL", "scancel"),
            sacct=os.getenv("REFLOW_SACCT", "sacct"),
            python=os.getenv("REFLOW_PYTHON", sys.executable),
        )

    def submit(self, resources: JobResources, command: list[str]) -> str:
        """Submit a command to Slurm and return the job identifier."""
        cmd = self._build_sbatch(resources, command)
        if self.mode == "dry-run":
            logger.info("DRY-RUN: %s", " ".join(cmd))
            return "DRYRUN"
        logger.debug("sbatch: %s", " ".join(cmd))
        output = subprocess.check_output(cmd, text=True).strip()
        logger.info("Submitted job %s", output)
        return output

    def cancel(self, job_id: str) -> None:
        """Cancel a previously submitted Slurm job."""
        if job_id == "DRYRUN":
            return
        try:
            subprocess.check_call([self.scancel, job_id])
        except subprocess.CalledProcessError as exc:
            logger.warning("scancel %s returned %d", job_id, exc.returncode)

    def job_state(self, job_id: str) -> str | None:
        """Return the current Slurm state for a job, if available."""
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

    def _build_sbatch(self, resources: JobResources, command: list[str]) -> list[str]:
        parts: list[str] = [
            self.sbatch,
            "--parsable",
            "--job-name",
            resources.job_name,
            "--cpus-per-task",
            str(resources.cpus),
            "--time",
            resources.time_limit,
            "--mem",
            resources.mem,
            "--partition",
            resources.partition,
        ]
        if resources.account:
            parts.extend(["-A", resources.account])
        if resources.array:
            parts.extend(["--array", resources.array])
        if resources.output_path is not None:
            parts.extend(["--output", str(resources.output_path)])
        if resources.error_path is not None:
            parts.extend(["--error", str(resources.error_path)])
        if resources.mail_user:
            parts.extend(["--mail-user", resources.mail_user])
        if resources.mail_type:
            parts.extend(["--mail-type", resources.mail_type])
        if resources.signal:
            parts.extend(["--signal", resources.signal])
        for key, value in resources.extra.items():
            flag = f"--{key}" if not key.startswith("-") else key
            parts.extend([flag, str(value)])
        parts.extend(["--wrap", shlex.join(command)])
        return parts
