"""SGE / UGE executor for reflow.

Supports Sun/Oracle Grid Engine, Univa Grid Engine, and
Open Grid Scheduler.  Uses ``qsub``/``qdel``/``qstat`` with
SGE-specific flag semantics (``-t`` for arrays,
``-hold_jid`` for dependencies).

# Note:

SGE shares command names (``qsub``, ``qdel``, ``qstat``) with PBS
but the flag syntax is different.  This executor must not be
confused with [`reflow.executors.pbs.PBSExecutor`][reflow.executors.pbs.PBSExecutor].

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


class SGEExecutor(Executor):
    """Submit, cancel, and query SGE / UGE jobs.

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
    python : str
        Python interpreter for worker jobs.
    config : Config or None
        User configuration.

    """

    array_index_env_var: str = "SGE_TASK_ID"
    _internal_keys: frozenset[str] = frozenset(
        {"python", "qsub", "qdel", "qstat", "mode"}
    )
    # SGE calls it "queue"; accept "partition" from Slurm users.
    # Also normalize Slurm-style "dependency" to SGE's "hold_jid".
    _KEY_ALIASES: dict[str, str] = {"partition": "queue"}

    def __init__(
        self,
        mode: str = "qsub",
        qsub: str = "qsub",
        qdel: str = "qdel",
        qstat: str = "qstat",
        python: str = "",
        config: Config | None = None,
    ) -> None:
        self.mode = mode
        self.qsub = qsub
        self.qdel = qdel
        self.qstat = qstat
        super().__init__(python=python, config=config)

    @classmethod
    def from_environment(cls, config: Config | None = None) -> SGEExecutor:
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
        # SGE qsub output varies:
        # "Your job 12345 ("name") has been submitted"
        # "Your job-array 12345.1-10:1 ("name") has been submitted"
        m = re.search(r"(?:job|job-array)\s+(\d+)", output)
        if m is None:
            logger.warning("Could not parse qsub output: %s", output)
            return output.split()[0] if output else output
        job_id = m.group(1)
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
                [self.qstat, "-j", job_id],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError:
            # Job no longer in qstat — likely completed or removed.
            return self._check_qacct(job_id)
        except FileNotFoundError:
            return None
        # If qstat returns successfully the job is still active.
        # Parse the state from the output.
        m = re.search(r"^job_state\s+\d+:\s*(\S+)", output, re.MULTILINE)
        if m is not None:
            state = m.group(1).lower()
            if "r" in state:
                return "RUNNING"
            if "q" in state or "w" in state or "h" in state:
                return "PENDING"
            if "e" in state:
                return "FAILED"
        return "RUNNING"  # default: still in qstat → running

    def _check_qacct(self, job_id: str) -> str | None:
        """Fall back to qacct for completed jobs."""
        try:
            output = subprocess.check_output(
                ["qacct", "-j", job_id],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None
        m = re.search(r"^exit_status\s+(\d+)", output, re.MULTILINE)
        if m is not None:
            return "COMPLETED" if m.group(1) == "0" else "FAILED"
        return "COMPLETED"

    def dependency_options(self, job_ids: list[str]) -> dict[str, str]:
        """SGE dependency: ``-hold_jid ID1,ID2``."""
        return {"hold_jid": ",".join(job_ids)}

    def _build_qsub(
        self,
        resources: JobResources,
        command: list[str],
    ) -> list[str]:
        parts: list[str] = [
            self.qsub,
            "-N",
            resources.job_name,
            "-pe",
            "smp",
            str(resources.cpus),
            "-l",
            f"h_rt={resources.time_limit}",
            "-l",
            f"h_vmem={resources.mem}",
            "-cwd",
            "-V",  # Export current environment.
        ]
        if resources.array:
            # SGE array: -t 0-9 or -t 1,3,7
            parts.extend(["-t", resources.array])
        if resources.output_path is not None:
            parts.extend(["-o", str(resources.output_path)])
        if resources.error_path is not None:
            parts.extend(["-e", str(resources.error_path)])

        for key, value in self._normalize_options(resources.submit_options).items():
            if key == "hold_jid":
                parts.extend(["-hold_jid", str(value)])
            elif key == "queue":
                parts.extend(["-q", str(value)])
            elif key == "account":
                parts.extend(["-A", str(value)])
            elif key == "dependency":
                # Slurm-style dependency key — translate to hold_jid.
                # Strip "afterany:" prefix if present.
                raw = str(value)
                if raw.startswith("afterany:"):
                    raw = raw[len("afterany:") :]
                parts.extend(["-hold_jid", raw.replace(":", ",")])
            else:
                parts.extend(self._render_submit_option(key, value))

        # SGE executes a script, not a wrapped command.
        # Use -b y (binary mode) to run an inline command.
        parts.extend(["-b", "y"])
        parts.append(shlex.join(command))
        return parts
