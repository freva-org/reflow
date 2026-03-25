"""LSF executor for reflow.

Supports IBM Spectrum LSF (``bsub``/``bkill``/``bjobs``).  LSF uses
bracket-based array syntax (``job[0-9]``) and expression-based
dependency specifications (``ended(ID1) && ended(ID2)``).
"""

from __future__ import annotations

import logging
import os
import re
import shlex
import subprocess
import sys

from ..config import Config
from . import Executor, JobResources

logger = logging.getLogger(__name__)


class LSFExecutor(Executor):
    """Submit, cancel, and query LSF jobs.

    Parameters
    ----------
    mode : str
        ``"bsub"`` for live submission, ``"dry-run"`` for logging only.
    bsub : str
        Path to the ``bsub`` binary.
    bkill : str
        Path to the ``bkill`` binary.
    bjobs : str
        Path to the ``bjobs`` binary.
    python : str
        Python interpreter for worker jobs.
    config : Config or None
        User configuration.

    """

    array_index_env_var: str = "LSB_JOBINDEX"
    _internal_keys: frozenset[str] = frozenset(
        {"python", "bsub", "bkill", "bjobs", "mode"}
    )
    # LSF calls it "queue"; accept "partition" from Slurm users.
    _KEY_ALIASES: dict[str, str] = {"partition": "queue"}

    def __init__(
        self,
        mode: str = "bsub",
        bsub: str = "bsub",
        bkill: str = "bkill",
        bjobs: str = "bjobs",
        python: str = "",
        config: Config | None = None,
    ) -> None:
        self.mode = mode
        self.bsub = bsub
        self.bkill = bkill
        self.bjobs = bjobs
        super().__init__(python=python, config=config)

    @classmethod
    def from_environment(cls, config: Config | None = None) -> LSFExecutor:
        """Build from ``REFLOW_*`` environment variables and config."""
        cfg = config or Config()
        mode = os.getenv("REFLOW_MODE") or cfg.executor_mode or "bsub"
        python = os.getenv("REFLOW_PYTHON") or cfg.executor_python or sys.executable
        return cls(
            mode=mode.strip().lower(),
            bsub=os.getenv("REFLOW_BSUB", "bsub"),
            bkill=os.getenv("REFLOW_BKILL", "bkill"),
            bjobs=os.getenv("REFLOW_BJOBS", "bjobs"),
            python=python,
            config=cfg,
        )

    def submit(self, resources: JobResources, command: list[str]) -> str:
        cmd = self._build_bsub(resources, command)
        if self.mode == "dry-run":
            logger.info("DRY-RUN: %s", " ".join(cmd))
            return "DRYRUN"
        logger.debug("bsub: %s", " ".join(cmd))
        output = subprocess.check_output(cmd, text=True).strip()
        # bsub output: "Job <12345> is submitted to queue <normal>."
        m = re.search(r"<(\d+)>", output)
        if m is None:
            logger.warning("Could not parse bsub output: %s", output)
            return output
        job_id = m.group(1)
        logger.info("Submitted job %s", job_id)
        return job_id

    def cancel(self, job_id: str) -> None:
        if job_id == "DRYRUN":
            return
        try:
            subprocess.check_call([self.bkill, job_id])
        except subprocess.CalledProcessError as exc:
            logger.warning("bkill %s returned %d", job_id, exc.returncode)

    def job_state(self, job_id: str) -> str | None:
        if job_id == "DRYRUN":
            return None
        try:
            output = subprocess.check_output(
                [self.bjobs, "-noheader", "-o", "stat", job_id],
                text=True, stderr=subprocess.DEVNULL,
            ).strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None
        if not output:
            return None
        state = output.splitlines()[0].strip().upper()
        _STATE_MAP = {
            "PEND": "PENDING",
            "RUN": "RUNNING",
            "DONE": "COMPLETED",
            "EXIT": "FAILED",
            "PSUSP": "SUSPENDED",
            "USUSP": "SUSPENDED",
            "SSUSP": "SUSPENDED",
            "WAIT": "PENDING",
            "UNKWN": None,
        }
        return _STATE_MAP.get(state, state)

    def dependency_options(self, job_ids: list[str]) -> dict[str, str]:
        """LSF dependency: ``-w "ended(ID1) && ended(ID2)"``."""
        expr = " && ".join(f"ended({jid})" for jid in job_ids)
        return {"dependency_expr": expr}

    def _build_bsub(
        self, resources: JobResources, command: list[str],
    ) -> list[str]:
        parts: list[str] = [self.bsub]

        # Job name — with array specification if needed.
        if resources.array:
            # LSF array syntax: -J "name[0-9]" or -J "name[1,3,7]"
            parts.extend(["-J", f"{resources.job_name}[{resources.array}]"])
        else:
            parts.extend(["-J", resources.job_name])

        parts.extend(["-n", str(resources.cpus)])
        parts.extend(["-W", resources.time_limit])
        parts.extend(["-M", resources.mem])

        if resources.output_path is not None:
            parts.extend(["-o", str(resources.output_path)])
        if resources.error_path is not None:
            parts.extend(["-e", str(resources.error_path)])

        for key, value in self._normalize_options(resources.submit_options).items():
            if key == "dependency_expr":
                parts.extend(["-w", str(value)])
            elif key == "queue":
                parts.extend(["-q", str(value)])
            elif key == "account":
                # LSF uses -G for user group / account.
                parts.extend(["-G", str(value)])
            else:
                parts.extend(self._render_submit_option(key, value))

        parts.append(shlex.join(command))
        return parts
