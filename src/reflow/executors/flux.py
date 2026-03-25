"""Flux executor for reflow.

Supports `Flux Framework <https://flux-framework.org/>`_, a modern
hierarchical resource manager gaining adoption at DOE national labs.

Flux uses ``flux submit`` / ``flux cancel`` / ``flux jobs`` with a
flag style that is close to Slurm, and supports ``--cc`` for array
(carbon-copy) jobs and ``--dependency`` for job dependencies.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys

from ..config import Config
from . import Executor, JobResources

logger = logging.getLogger(__name__)


class FluxExecutor(Executor):
    """Submit, cancel, and query Flux jobs.

    Parameters
    ----------
    mode : str
        ``"flux"`` for live submission, ``"dry-run"`` for logging only.
    flux : str
        Path to the ``flux`` binary.
    python : str
        Python interpreter for worker jobs.
    config : Config or None
        User configuration.

    """

    array_index_env_var: str = "FLUX_JOB_CC"
    _internal_keys: frozenset[str] = frozenset({"python", "flux", "mode"})
    # Flux calls it "queue"; accept "partition" from Slurm users.
    _KEY_ALIASES: dict[str, str] = {"partition": "queue"}

    def __init__(
        self,
        mode: str = "flux",
        flux: str = "flux",
        python: str = "",
        config: Config | None = None,
    ) -> None:
        self.mode = mode
        self.flux = flux
        super().__init__(python=python, config=config)

    @classmethod
    def from_environment(cls, config: Config | None = None) -> FluxExecutor:
        """Build from ``REFLOW_*`` environment variables and config."""
        cfg = config or Config()
        mode = os.getenv("REFLOW_MODE") or cfg.executor_mode or "flux"
        python = os.getenv("REFLOW_PYTHON") or cfg.executor_python or sys.executable
        return cls(
            mode=mode.strip().lower(),
            flux=os.getenv("REFLOW_FLUX", "flux"),
            python=python,
            config=cfg,
        )

    def submit(self, resources: JobResources, command: list[str]) -> str:
        cmd = self._build_submit(resources, command)
        if self.mode == "dry-run":
            logger.info("DRY-RUN: %s", " ".join(cmd))
            return "DRYRUN"
        logger.debug("flux submit: %s", " ".join(cmd))
        output = subprocess.check_output(cmd, text=True).strip()
        # flux submit prints the job ID (an f-encoded integer).
        job_id = output.splitlines()[-1].strip() if output else output
        logger.info("Submitted job %s", job_id)
        return job_id

    def cancel(self, job_id: str) -> None:
        if job_id == "DRYRUN":
            return
        try:
            subprocess.check_call([self.flux, "cancel", job_id])
        except subprocess.CalledProcessError as exc:
            logger.warning("flux cancel %s returned %d", job_id, exc.returncode)

    def job_state(self, job_id: str) -> str | None:
        if job_id == "DRYRUN":
            return None
        try:
            output = subprocess.check_output(
                [
                    self.flux,
                    "jobs",
                    "--no-header",
                    "-o",
                    "{status_abbrev}",
                    job_id,
                ],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None
        if not output:
            return None
        state = output.splitlines()[0].strip().upper()
        _STATE_MAP = {
            "PD": "PENDING",
            "R": "RUNNING",
            "CD": "COMPLETED",
            "F": "FAILED",
            "CA": "CANCELLED",
            "S": "SUSPENDED",
            "I": "INACTIVE",
        }
        return _STATE_MAP.get(state, state)

    def dependency_options(self, job_ids: list[str]) -> dict[str, str]:
        """Flux dependency: ``--dependency=afterany:ID1,ID2``."""
        return {"dependency": "afterany:" + ",".join(job_ids)}

    def _build_submit(
        self,
        resources: JobResources,
        command: list[str],
    ) -> list[str]:
        parts: list[str] = [
            self.flux,
            "submit",
            "--job-name",
            resources.job_name,
            "--cores",
            str(resources.cpus),
            "-t",
            resources.time_limit,
        ]

        # Flux doesn't have a direct --mem flag; memory is typically
        # controlled via the resource spec.  Pass it as an attribute
        # if set, which users can customise.
        if resources.mem:
            parts.extend(["--setattr=system.alloc.mem=" + resources.mem])

        if resources.array:
            # Flux carbon-copy: --cc=0-9 or --cc=1,3,7
            parts.extend([f"--cc={resources.array}"])

        if resources.output_path is not None:
            parts.extend(["--output", str(resources.output_path)])
        if resources.error_path is not None:
            parts.extend(["--error", str(resources.error_path)])

        for key, value in self._normalize_options(resources.submit_options).items():
            if key == "dependency":
                parts.extend([f"--dependency={value}"])
            elif key == "queue":
                parts.extend(["--queue", str(value)])
            elif key == "account":
                parts.extend(["--setattr=system.bank=" + str(value)])
            else:
                parts.extend(self._render_submit_option(key, value))

        parts.extend(command)
        return parts
