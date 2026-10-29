"""Slurm executor for reflow."""

from __future__ import annotations

import logging
import os
import re
import shlex
import sys

from ..config import Config
from . import ElementState, Executor, JobResources, JobStates, JobStatus
from .util import ExecutorError, run_cmd

logger = logging.getLogger(__name__)


class SlurmExecutor(Executor):
    """Submit, cancel, and query Slurm jobs."""

    array_index_env_var: str = "SLURM_ARRAY_TASK_ID"
    _internal_keys: frozenset[str] = frozenset(
        {"python", "sbatch", "sacct", "scancel", "squeue", "mode"}
    )
    # Slurm calls it "partition"; accept "queue" from PBS/LSF/SGE users.
    _KEY_ALIASES: dict[str, str] = {"queue": "partition"}

    def __init__(
        self,
        mode: str = "sbatch",
        sbatch: str = "sbatch",
        scancel: str = "scancel",
        sacct: str = "sacct",
        python: str = "",
        config: Config | None = None,
        squeue: str = "squeue",
    ) -> None:
        self.mode = mode
        self.sbatch = sbatch
        self.scancel = scancel
        self.sacct = sacct
        self.squeue = squeue
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
            squeue=os.getenv("REFLOW_SQUEUE", "squeue"),
        )

    def submit(self, resources: JobResources, command: list[str]) -> str:
        cmd = self._build_sbatch(resources, command)
        if self.mode == "dry-run":
            logger.info("DRY-RUN: %s", " ".join(cmd))
            return "DRYRUN"
        logger.debug("sbatch: %s", " ".join(cmd))
        output = run_cmd(cmd)
        logger.info("Submitted job %s", output.stdout)
        return output.stdout

    def cancel(self, job_id: str) -> None:
        if job_id == "DRYRUN":
            return
        try:
            run_cmd([self.scancel, job_id])
        except ExecutorError as exc:
            logger.warning("scancel %s returned %d", job_id, exc.result.returncode)

    def job_state(self, job_id: str) -> str | None:
        if job_id == "DRYRUN":
            return None
        try:
            output = run_cmd(
                [
                    self.sacct,
                    "-j",
                    job_id,
                    "--noheader",
                    "--parsable2",
                    "--format=State",
                ],
            )
        except ExecutorError:
            return None
        lines = [ln.strip() for ln in output.stdout.splitlines() if ln.strip()]
        return lines[0] if lines else None

    def job_states(self, job_id: str) -> JobStates:
        """Per-element states from ``sacct``, falling back to ``squeue``.

        ``sacct`` lists every element, including pending ranges such as
        ``123_[4-9%2]``, and says how finished ones ended.  Where
        accounting is unavailable or has not caught up, ``squeue`` lists
        the live elements; anything it does not list has ended (without
        a known reason).
        """
        if job_id == "DRYRUN":
            return JobStates()
        base = job_id.split(";", 1)[0].strip()
        states = self._sacct_states(base)
        if states is not None:
            return states
        return self._squeue_states(base)

    def job_status(self, job_id: str) -> ElementState:
        """Ask ``squeue`` whether any element of *job_id* is still queued.

        ``squeue`` only lists live jobs, so a job it does not list (or
        calls an invalid job id) has ended.  No accounting needed.
        """
        if job_id == "DRYRUN":
            return ElementState(JobStatus.ENDED, "dry-run")
        return self._squeue_states(job_id.split(";", 1)[0].strip()).overall()

    def _sacct_states(self, job_id: str) -> JobStates | None:
        try:
            output = run_cmd(
                [
                    self.sacct,
                    "-j",
                    job_id,
                    "--allocations",
                    "--noheader",
                    "--parsable2",
                    "--format=JobID,State",
                ]
            )
        except (ExecutorError, OSError):
            return None
        elements = _parse_job_lines(output.stdout, job_id)
        return JobStates(elements=elements) if elements else None

    def _squeue_states(self, job_id: str) -> JobStates:
        try:
            output = run_cmd(
                [self.squeue, "-j", job_id, "-r", "-h", "-o", "%i|%T"], check=False
            )
        except OSError:
            return JobStates()
        if output.returncode != 0:
            # A job Slurm no longer knows about has ended long ago.
            if "invalid job id" in output.stderr.lower():
                return JobStates(default=ElementState(JobStatus.ENDED))
            return JobStates()
        return JobStates(
            elements=_parse_job_lines(output.stdout, job_id),
            default=ElementState(JobStatus.ENDED),
        )

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
        for key, value in self._normalize_options(resources.submit_options).items():
            parts.extend(self._render_submit_option(key, value))

        parts.extend(["--wrap", shlex.join(command)])
        return parts


# --- state parsing ---------------------------------------------------------

_QUEUED = frozenset(
    {
        "PENDING",
        "REQUEUED",
        "REQUEUE_HOLD",
        "REQUEUE_FED",
        "SUSPENDED",
        "CONFIGURING",
        "RESIZING",
        "STOPPED",
    }
)
_RUNNING = frozenset(
    {"RUNNING", "COMPLETING", "STAGE_OUT", "SIGNALING", "RESV_DEL_HOLD"}
)
_ENDED = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "PREEMPTED",
        "BOOT_FAIL",
        "DEADLINE",
        "REVOKED",
        "SPECIAL_EXIT",
    }
)
_JOB_ID = re.compile(r"^(?P<job>\d+)(?:_(?P<index>\d+|\[[^\]]*\]))?$")


def _normalise_state(raw: str) -> ElementState:
    """Map a Slurm state such as ``"CANCELLED by 1234"`` to an `ElementState`."""
    word = raw.strip().split(" ", 1)[0].rstrip("+").upper()
    if word in _QUEUED:
        return ElementState(JobStatus.QUEUED, word)
    if word in _RUNNING:
        return ElementState(JobStatus.RUNNING, word)
    if word in _ENDED:
        return ElementState(JobStatus.ENDED, word)
    return ElementState(JobStatus.UNKNOWN, word)


def _expand_indices(spec: str) -> list[int]:
    """Expand ``"[0-3,7,9-10%2]"`` (or ``"5"``) into array indices."""
    spec = spec.strip("[]").split("%", 1)[0]
    indices: list[int] = []
    for part in filter(None, spec.split(",")):
        if "-" in part:
            start, stop = part.split("-", 1)
            indices.extend(range(int(start), int(stop) + 1))
        else:
            indices.append(int(part))
    return indices


def _parse_job_lines(text: str, job_id: str) -> dict[int | None, ElementState]:
    """Parse ``JobID|State`` lines of ``sacct`` or ``squeue`` for *job_id*.

    Lines of other jobs and of job steps (``123_4.batch``) are skipped.
    """
    elements: dict[int | None, ElementState] = {}
    for line in text.splitlines():
        if "|" not in line:
            continue
        ident, raw = line.split("|", 1)
        match = _JOB_ID.match(ident.strip())
        if match is None or match["job"] != job_id.split("_", 1)[0]:
            continue
        state = _normalise_state(raw)
        if match["index"] is None:
            elements[None] = state
            continue
        for index in _expand_indices(match["index"]):
            elements[index] = state
    return elements
