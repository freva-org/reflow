"""Abstract executor interface for reflow."""

from __future__ import annotations

import abc
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
        **scheduler_options: Any,
    ) -> None:
        merged = dict(submit_options or {})
        if extra:
            merged.update(extra)
        merged.update(scheduler_options)

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
    """Base class for workload-manager executors."""

    @abc.abstractmethod
    def submit(self, resources: JobResources, command: list[str]) -> str:
        """Submit a job and return its identifier."""

    @abc.abstractmethod
    def cancel(self, job_id: str) -> None:
        """Cancel a submitted or running job."""

    @abc.abstractmethod
    def job_state(self, job_id: str) -> str | None:
        """Query the current state of a job."""
