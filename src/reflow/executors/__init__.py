"""Abstract executor interface for reflow."""

from __future__ import annotations

import abc
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import Config


@dataclass(init=False)
class JobResources:
    """Resource request for one task submission."""

    job_name: str
    cpus: int
    time_limit: str
    mem: str
    array: str | None
    output_path: Path | None
    error_path: Path | None
    submit_options: dict[str, Any]
    backend: str | None

    def __init__(
        self,
        job_name: str = "",
        cpus: int = 1,
        time_limit: str = "00:30:00",
        mem: str = "4G",
        array: str | None = None,
        output_path: Path | None = None,
        error_path: Path | None = None,
        submit_options: dict[str, Any] | None = None,
        backend: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        merged: dict[str, Any] = dict(submit_options or {})
        if extra:
            merged.update(extra)

        self.job_name = job_name
        self.cpus = cpus
        self.time_limit = time_limit
        self.mem = mem
        self.array = array
        self.output_path = output_path
        self.error_path = error_path
        self.submit_options = merged
        self.backend = backend

    @property
    def extra(self) -> dict[str, Any]:
        """Backward-compatible alias for scheduler-native submit options."""
        return self.submit_options

    @property
    def partition(self) -> str | None:
        return self.submit_options.get("partition")

    @property
    def account(self) -> str | None:
        return self.submit_options.get("account")

    @property
    def mail_user(self) -> str | None:
        return self.submit_options.get("mail_user")

    @property
    def mail_type(self) -> str | None:
        return self.submit_options.get("mail_type")

    @property
    def signal(self) -> str | None:
        return self.submit_options.get("signal")


class Executor(abc.ABC):
    """Base class for workload-manager executors.

    Subclasses must implement [`submit`][submit], [`cancel`][cancel], and
    [`job_state`][job_state].  The `array_index_env_var`
    class attribute tells the worker which environment variable carries the
    array task index at runtime.

    Key normalisation
    -----------------
    Users write scheduler-agnostic config (e.g. ``partition = "gpu"``
    or ``queue = "batch"``).  Each executor declares a
    :attr:`_KEY_ALIASES` mapping that rewrites incoming keys to the
    backend's native vocabulary before rendering CLI flags.  This
    means ``partition`` and ``queue`` are treated as synonyms — a
    user never needs to know which scheduler is active.
    """

    #: Environment variable that the scheduler sets to the array task
    #: index inside a running job.  Overridden by each backend.
    array_index_env_var: str = "SLURM_ARRAY_TASK_ID"

    #: Keys in *submit_options* that are internal to reflow and should
    #: not be rendered as scheduler flags.
    _internal_keys: frozenset[str] = frozenset({"python", "mode"})

    #: Map from alias keys to this backend's native key.
    #: For example, Slurm maps ``{"queue": "partition"}`` so that a
    #: PBS user's ``queue = "batch"`` becomes ``--partition batch``.
    #: Subclasses override this.
    _KEY_ALIASES: dict[str, str] = {}

    def __init__(
        self, mode: str = "", python: str = "", config: Config | None = None
    ) -> None:
        self.python = python or sys.executable
        self._config = config or Config()

    @abc.abstractmethod
    def submit(self, resources: JobResources, command: list[str]) -> str:
        """Submit a job and return its identifier."""

    @abc.abstractmethod
    def cancel(self, job_id: str) -> None:
        """Cancel a submitted or running job."""

    @abc.abstractmethod
    def job_state(self, job_id: str) -> str | None:
        """Query the current state of a job."""

    def dependency_options(self, job_ids: list[str]) -> dict[str, str]:
        """Return *submit_options* entries for depending on *job_ids*.

        The dispatch loop calls this to build the dependency specification
        for the follow-up dispatch job.  Each backend returns the correct
        key/value pair for its scheduler.

        The default implementation produces Slurm-style
        ``{"dependency": "afterany:ID1:ID2"}``.

        Parameters
        ----------
        job_ids : list[str]
            Job identifiers to depend on.

        Returns
        -------
        dict[str, str]
            Entries to merge into *submit_options*.

        """
        return {"dependency": "afterany:" + ":".join(job_ids)}

    def _normalize_options(self, options: dict[str, Any]) -> dict[str, Any]:
        """Rewrite alias keys to this backend's native vocabulary.

        Applies :attr:`_KEY_ALIASES`, skips :attr:`_internal_keys`,
        and returns a new dict.  The original is not mutated.

        For example, on a PBS executor with
        ``_KEY_ALIASES = {"partition": "queue"}``, an incoming
        ``{"partition": "gpu"}`` becomes ``{"queue": "gpu"}``.
        If both the alias and the native key are present, the
        native key wins (it is more specific).
        """
        if not self._KEY_ALIASES:
            return {k: v for k, v in options.items() if k not in self._internal_keys}
        out: dict[str, Any] = {}
        for key, value in options.items():
            if key in self._internal_keys:
                continue
            native = self._KEY_ALIASES.get(key, key)
            # Don't overwrite a native key that's already present.
            if native in out and key != native:
                continue
            out[native] = value
        return out

    def _render_submit_option(self, key: str, value: Any) -> list[str]:
        """Render a single submit option as CLI flags.

        Handles booleans, lists, ``None``, and scalar values.
        Keys are converted from ``snake_case`` to ``--kebab-case``.
        """
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
