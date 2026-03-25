"""PBS executor for reflow (PBS Pro, OpenPBS, Torque).

Supports both PBS Pro (``-J`` array flag) and Torque (``-t`` array
flag).  The variant is auto-detected from ``qstat --version`` output
or can be set explicitly via ``array_flag``.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys

from ..config import Config
from . import Executor, JobResources

logger = logging.getLogger(__name__)

# PBS Pro uses -J, Torque uses -t for array jobs.
_ARRAY_FLAG_PBS_PRO = "-J"
_ARRAY_FLAG_TORQUE = "-t"


def _detect_pbs_variant(qstat: str = "qstat") -> str:
    """Return ``"-J"`` for PBS Pro / OpenPBS or ``"-t"`` for Torque."""
    try:
        out = subprocess.check_output(
            [qstat, "--version"],
            text=True,
            stderr=subprocess.STDOUT,
        ).lower()
        if "pbs pro" in out or "openpbs" in out:
            return _ARRAY_FLAG_PBS_PRO
        return _ARRAY_FLAG_TORQUE
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        # Default to PBS Pro if we can't detect.
        return _ARRAY_FLAG_PBS_PRO


class PBSExecutor(Executor):
    """Submit, cancel, and query PBS / Torque jobs.

    Parameters
    ----------
    mode : str
        ``"qsub"`` for live submission, ``"dry-run"`` for logging only.
    qsub : str
        Path to the ``qsub`` binary.
    qdel : str
        Path to the ``qdel`` binary.
    qstat : str
        Path to the ``qstat`` binary.
    array_flag : str or None
        ``"-J"`` for PBS Pro / OpenPBS, ``"-t"`` for Torque.
        Auto-detected when ``None``.
    python : str
        Python interpreter for worker jobs.
    config : Config or None
        User configuration.

    """

    array_index_env_var: str = "PBS_ARRAY_INDEX"
    _internal_keys: frozenset[str] = frozenset(
        {"python", "qsub", "qdel", "qstat", "mode"}
    )
    # PBS calls it "queue"; accept "partition" from Slurm users.
    _KEY_ALIASES: dict[str, str] = {"partition": "queue"}

    def __init__(
        self,
        mode: str = "qsub",
        qsub: str = "qsub",
        qdel: str = "qdel",
        qstat: str = "qstat",
        array_flag: str | None = None,
        python: str = "",
        config: Config | None = None,
    ) -> None:
        self.mode = mode
        self.qsub = qsub
        self.qdel = qdel
        self.qstat = qstat
        self.array_flag = array_flag or _detect_pbs_variant(qstat)
        super().__init__(python=python, config=config)

    @classmethod
    def from_environment(cls, config: Config | None = None) -> PBSExecutor:
        """Build from ``REFLOW_*`` environment variables and config."""
        cfg = config or Config()
        mode = os.getenv("REFLOW_MODE") or cfg.executor_mode or "qsub"
        python = os.getenv("REFLOW_PYTHON") or cfg.executor_python or sys.executable
        return cls(
            mode=mode.strip().lower(),
            qsub=os.getenv("REFLOW_QSUB", "qsub"),
            qdel=os.getenv("REFLOW_QDEL", "qdel"),
            qstat=os.getenv("REFLOW_QSTAT", "qstat"),
            python=python,
            config=cfg,
        )

    def submit(self, resources: JobResources, command: list[str]) -> str:
        cmd = self._build_qsub(resources, command)
        if self.mode == "dry-run":
            logger.info("DRY-RUN: %s", " ".join(cmd))
            return "DRYRUN"
        logger.debug("qsub: %s", " ".join(cmd))
        output = subprocess.check_output(cmd, text=True).strip()
        # qsub returns a job ID like "12345.server" — take the numeric part.
        job_id = output.split(".")[0].strip()
        logger.info("Submitted job %s", job_id)
        return job_id

    def cancel(self, job_id: str) -> None:
        if job_id == "DRYRUN":
            return
        try:
            subprocess.check_call([self.qdel, job_id])
        except subprocess.CalledProcessError as exc:
            logger.warning("qdel %s returned %d", job_id, exc.returncode)

    def job_state(self, job_id: str) -> str | None:
        if job_id == "DRYRUN":
            return None
        try:
            output = subprocess.check_output(
                [self.qstat, "-f", "-x", job_id],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None
        # Parse "job_state = X" from qstat -f output.
        m = re.search(r"job_state\s*=\s*(\S+)", output)
        if m is None:
            return None
        state = m.group(1).upper()
        _STATE_MAP = {
            "Q": "PENDING",
            "H": "PENDING",
            "W": "PENDING",
            "R": "RUNNING",
            "E": "RUNNING",
            "T": "RUNNING",
            "F": "COMPLETED",
            "C": "COMPLETED",
            "S": "SUSPENDED",
        }
        return _STATE_MAP.get(state, state)

    def dependency_options(self, job_ids: list[str]) -> dict[str, str]:
        """PBS dependency: ``-W depend=afterany:ID1:ID2``."""
        return {"depend": "afterany:" + ":".join(job_ids)}

    def _build_qsub(
        self,
        resources: JobResources,
        command: list[str],
    ) -> list[str]:
        parts: list[str] = [
            self.qsub,
            "-N",
            resources.job_name,
            "-l",
            f"ncpus={resources.cpus}",
            "-l",
            f"walltime={resources.time_limit}",
            "-l",
            f"mem={resources.mem}",
        ]
        if resources.array:
            parts.extend([self.array_flag, resources.array])
        if resources.output_path is not None:
            parts.extend(["-o", str(resources.output_path)])
        if resources.error_path is not None:
            parts.extend(["-e", str(resources.error_path)])

        # Keys are normalized: "partition" → "queue", internals stripped.
        for key, value in self._normalize_options(resources.submit_options).items():
            if key == "depend":
                parts.extend(["-W", f"depend={value}"])
            elif key == "queue":
                parts.extend(["-q", str(value)])
            elif key == "account":
                parts.extend(["-A", str(value)])
            else:
                parts.extend(self._render_submit_option(key, value))

        parts.append("--")
        parts.extend(command)
        return parts
