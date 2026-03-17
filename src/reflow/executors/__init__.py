"""Abstract executor interface for reflow."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class JobResources:
    """Resource request for one task submission."""

    job_name: str = ""
    cpus: int = 1
    time_limit: str = "00:30:00"
    mem: str = "4G"
    partition: str = "compute"
    account: str | None = None
    array: str | None = None
    output_path: Path | None = None
    error_path: Path | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    mail_user: str | None = None
    mail_type: str | None = None
    signal: str | None = None


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
