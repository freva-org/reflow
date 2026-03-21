"""Slurm executor for reflow."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
from typing import Any

from ..config import Config
from . import Executor, JobResources

logger = logging.getLogger(__name__)


class SlurmExecutor(Executor):
    """Submit, cancel, and query Slurm jobs."""

    def __init__(
        self,
        mode: str = "sbatch",
        sbatch: str = "sbatch",
        scancel: str = "scancel",
        sacct: str = "sacct",
        python: str = "",
        config: Config | None = None,
    ) -> None:
        self.mode = mode
        self.sbatch = sbatch
        self.scancel = scancel
        self.sacct = sacct
        super().__init__(python=python, config=config)

    @classmethod
    def from_environment(cls, config: Config | None = None) -> SlurmExecutor:
        """Build from ``REFLOW_*`` environment variables and config."""
        cfg = config or Config()
        mode = os.getenv("REFLOW_MODE") or cfg.executor_mode or "sbatch"
        python = os.getenv("REFLOW_PYTHON") or cfg.executor_python or sys.executable
        return cls(
            mode=mode.strip().lower(),
            sbatch=os.getenv("REFLOW_SBATCH", "sbatch"),
            scancel=os.getenv("REFLOW_SCANCEL", "scancel"),
            sacct=os.getenv("REFLOW_SACCT", "sacct"),
            python=python,
        )

    def submit(self, resources: JobResources, command: list[str]) -> str:
        cmd = self._build_sbatch(resources, command)
        if self.mode == "dry-run":
            logger.info("DRY-RUN: %s", " ".join(cmd))
            return "DRYRUN"
        logger.debug("sbatch: %s", " ".join(cmd))
        output = subprocess.check_output(cmd, text=True).strip()
        logger.info("Submitted job %s", output)
        return output

    def cancel(self, job_id: str) -> None:
        if job_id == "DRYRUN":
            return
        try:
            subprocess.check_call([self.scancel, job_id])
        except subprocess.CalledProcessError as exc:
            logger.warning("scancel %s returned %d", job_id, exc.returncode)

    def job_state(self, job_id: str) -> str | None:
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
        ]
        if resources.array:
            parts.extend(["--array", resources.array])
        if resources.output_path is not None:
            parts.extend(["--output", str(resources.output_path)])
        if resources.error_path is not None:
            parts.extend(["--error", str(resources.error_path)])
        for key, value in resources.submit_options.items():
            if key not in ("python", "sbatch", "sacct", "scancel", "mode"):
                parts.extend(self._render_submit_option(key, value))

        parts.extend(["--wrap", shlex.join(command)])
        return parts

    def _render_submit_option(self, key: str, value: Any) -> list[str]:
        if value is None or value is False:
            return []
        flag = (
            f"--{key.replace('_', '-')}" if not str(key).startswith("-") else str(key)
        )
        if value is True:
            return [flag]
        if isinstance(value, (list, tuple)):
            rendered: list[str] = []
            for item in value:
                rendered.extend([flag, str(item)])
            return rendered
        return [flag, str(value)]
