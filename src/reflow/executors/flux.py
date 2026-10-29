"""Flux executor for reflow.

Supports `Flux Framework <https://flux-framework.org/>`_, a modern
hierarchical resource manager gaining adoption at DOE national labs.

Flux uses ``flux submit`` / ``flux cancel`` / ``flux jobs`` with a
flag style that is close to Slurm.  Two differences matter to reflow:

- ``--cc=IDSET`` (carbon copy) creates one *separate job* per index,
  with the index in ``FLUX_JOB_CC``; there is no array job id.  reflow
  therefore keeps every element's id: `submit` returns them comma
  separated, and `element_job_ids` maps them to array indices, using a
  job name that ends in the index (``<name>.{cc}``).
- ``--dependency`` takes one job id; it is repeated per job.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from typing import Any

from ..config import Config
from . import ElementState, Executor, JobResources, JobStates, JobStatus, summarise
from .util import ExecutorError, run_cmd

#: A Flux job id as printed by ``flux submit``: F58 (``ƒ…`` or, with
#: ``FLUX_F58_FORCE_ASCII``, ``f…``), or decimal.
_JOB_ID = re.compile(r"^(?:[ƒf][1-9A-HJ-NP-Za-km-z]+|\d+)$")

#: Most job ids passed to one ``flux cancel`` / ``flux jobs`` call.
_BATCH = 200

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
        logger.debug("flux submit: %s", " ".join(cmd[:12]))
        output = run_cmd(cmd).stdout
        ids = [ln.strip() for ln in output.splitlines() if _JOB_ID.match(ln.strip())]
        if not ids:
            # Unexpected output: keep the old behaviour (last line).
            return output.splitlines()[-1].strip() if output else output
        if not resources.array:
            logger.info("Submitted job %s", ids[-1])
            return ids[-1]
        # One job per element: return all ids, ordered by array index.
        ordered = self._order_by_index(ids, resources)
        logger.info(
            "Submitted %d jobs (%s ... %s)", len(ordered), ordered[0], ordered[-1]
        )
        return ",".join(ordered)

    def _order_by_index(self, ids: list[str], resources: JobResources) -> list[str]:
        """Sort element ids by array index, read from their ``.{cc}`` names."""
        try:
            listing = run_cmd(
                [self.flux, "jobs", "-a", "--no-header", "-o", "{id.f58} {name}", *ids]
            ).stdout
        except (ExecutorError, OSError):
            listing = ""
        index_of: dict[str, int] = {}
        suffix = re.compile(re.escape(resources.job_name) + r"\.(\d+)$")
        for line in listing.splitlines():
            parts = line.split()
            if len(parts) == 2 and (match := suffix.search(parts[1])):
                index_of[parts[0]] = int(match.group(1))
        if len(index_of) == len(ids):
            return sorted(ids, key=index_of.__getitem__)
        # Fall back to the order flux printed them in (submission order).
        logger.debug("Could not map all Flux job names to indices; using print order")
        return ids

    def element_job_ids(self, job_id: str, indices: list[int]) -> dict[int, str] | None:
        """Map the comma-separated ids from `submit` to array indices."""
        ids = [part for part in job_id.split(",") if part]
        if len(ids) < 2 and len(indices) < 2:
            return {indices[0]: ids[0]} if ids and indices else None
        if len(ids) != len(indices):
            logger.warning(
                "Flux returned %d job ids for %d array elements; "
                "keeping them as one group",
                len(ids),
                len(indices),
            )
            return None
        return dict(zip(sorted(indices), ids, strict=True))

    def cancel(self, job_id: str) -> None:
        self.cancel_many([job_id])

    def cancel_many(self, job_ids: list[str]) -> None:
        """Cancel all given jobs (comma-separated groups included) in batches."""
        ids = [i for jid in job_ids for i in jid.split(",") if i and i != "DRYRUN"]
        for start in range(0, len(ids), _BATCH):
            batch = ids[start : start + _BATCH]
            try:
                run_cmd([self.flux, "cancel", *batch])
            except ExecutorError as exc:
                logger.warning(
                    "flux cancel of %d job(s) returned %d",
                    len(batch),
                    exc.result.returncode,
                )

    def job_states(self, job_id: str) -> JobStates:
        """States of one job or a comma-separated group from `submit`.

        Uses ``flux jobs -a`` (including inactive jobs) with the job state
        and result.  Elements are keyed by the index in their job name;
        a single job is keyed ``None``.
        """
        ids = [i for i in job_id.split(",") if i and i != "DRYRUN"]
        if not ids:
            return JobStates()
        elements: dict[int | None, ElementState] = {}
        for start in range(0, len(ids), _BATCH):
            batch = ids[start : start + _BATCH]
            try:
                output = run_cmd(
                    [
                        self.flux,
                        "jobs",
                        "-a",
                        "--no-header",
                        "-o",
                        "{id.f58} {state} {result} {name}",
                        *batch,
                    ],
                    check=False,
                )
            except OSError:
                return JobStates()
            if output.returncode != 0 and not output.stdout:
                return JobStates()
            for line in output.stdout.splitlines():
                parts = line.split()
                if len(parts) < 2:
                    continue
                state = _normalise_state(parts[1], parts[2] if len(parts) > 3 else "")
                index = _index_from_name(parts[-1]) if len(ids) > 1 else None
                elements[index] = state
        return JobStates(elements=elements)

    def job_state(self, job_id: str) -> str | None:
        if job_id == "DRYRUN":
            return None
        try:
            output = run_cmd(
                [
                    self.flux,
                    "jobs",
                    "--no-header",
                    "-o",
                    "{status_abbrev}",
                    job_id,
                ],
            ).stdout.strip()
        except (ExecutorError, FileNotFoundError):
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

    def job_status(self, job_id: str) -> ElementState:
        """Tell whether any job of *job_id* (one id or a group) is alive.

        Asks ``flux jobs`` for each id.  Ids Flux reports as unknown
        (purged from its job list) have ended.
        """
        ids = [i for i in job_id.split(",") if i and i != "DRYRUN"]
        if not ids:
            return ElementState(JobStatus.ENDED, "dry-run")
        states: list[ElementState] = []
        for start in range(0, len(ids), _BATCH):
            batch = ids[start : start + _BATCH]
            try:
                output = run_cmd(
                    [
                        self.flux,
                        "jobs",
                        "-a",
                        "--no-header",
                        "-o",
                        "{id.f58} {state} {result}",
                        *batch,
                    ],
                    check=False,
                )
            except OSError:
                return ElementState(JobStatus.UNKNOWN, "flux jobs failed")
            listed = {}
            for line in output.stdout.splitlines():
                parts = line.split()
                if len(parts) >= 2:
                    result = parts[2] if len(parts) > 2 else ""
                    listed[_ascii_id(parts[0])] = _normalise_state(parts[1], result)
            # flux jobs reports each id it no longer knows on stderr
            # ("JobID ƒ… unknown").  Anything else (broker unreachable)
            # leaves the unlisted ids unknown.
            purged = {
                _ascii_id(word.strip(":,"))
                for line in output.stderr.splitlines()
                if "unknown" in line.lower()
                for word in line.split()
            }
            for job in batch:
                key = _ascii_id(job)
                state = listed.get(key)
                if state is None:
                    ended = output.returncode == 0 or key in purged
                    state = ElementState(
                        JobStatus.ENDED if ended else JobStatus.UNKNOWN
                    )
                states.append(state)
        return summarise(states)

    def dependency_options(self, job_ids: list[str]) -> dict[str, Any]:
        """One ``--dependency=afterany:ID`` per job; Flux takes one id each.

        Comma-separated groups returned by `submit` (one id per array
        element) are expanded, so the follow-up waits for every element.
        """
        ids = [i for jid in job_ids for i in jid.split(",") if i]
        return {"dependency": [f"afterany:{i}" for i in ids]}

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
            # Flux carbon-copy: --cc=0-9 or --cc=1,3,7 creates one job per
            # index.  The index in the name lets reflow map ids to indices.
            idset, _, limit = resources.array.partition("%")
            if limit:
                logger.warning(
                    "Flux has no per-array concurrency limit; ignoring "
                    "array_parallelism=%s",
                    limit,
                )
            parts.extend([f"--cc={idset}"])
            parts[parts.index("--job-name") + 1] = f"{resources.job_name}.{{cc}}"

        if resources.output_path is not None:
            parts.extend(["--output", str(resources.output_path)])
        if resources.error_path is not None:
            parts.extend(["--error", str(resources.error_path)])

        for key, value in self._normalize_options(resources.submit_options).items():
            if key == "dependency":
                values = value if isinstance(value, (list, tuple)) else [value]
                parts.extend(f"--dependency={item}" for item in values)
            elif key == "queue":
                parts.extend(["--queue", str(value)])
            elif key == "account":
                parts.extend(["--setattr=system.bank=" + str(value)])
            else:
                parts.extend(self._render_submit_option(key, value))

        parts.extend(command)
        return parts


_QUEUED = frozenset({"NEW", "DEPEND", "PRIORITY", "SCHED"})
_RUNNING = frozenset({"RUN", "CLEANUP"})


def _normalise_state(state: str, result: str) -> ElementState:
    """Map Flux's ``{state}`` / ``{result}`` to an `ElementState`."""
    word = state.strip().upper()
    if word in _QUEUED:
        return ElementState(JobStatus.QUEUED, word)
    if word in _RUNNING:
        return ElementState(JobStatus.RUNNING, word)
    if word == "INACTIVE":
        return ElementState(JobStatus.ENDED, result.strip().upper())
    return ElementState(JobStatus.UNKNOWN, word)


def _index_from_name(name: str) -> int | None:
    """Array index from a ``<name>.{cc}`` job name, if present."""
    head, _, tail = name.rpartition(".")
    return int(tail) if head and tail.isdigit() else None


def _ascii_id(job_id: str) -> str:
    """``ƒAbc`` and ``fAbc`` name the same job (``FLUX_F58_FORCE_ASCII``)."""
    return "f" + job_id[1:] if job_id.startswith("ƒ") else job_id
