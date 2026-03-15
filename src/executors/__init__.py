"""Abstract executor interface for slurm_flow.

Every workload-manager backend implements this interface so that the
orchestration core stays backend-agnostic.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


@dataclass
class JobResources:
    """Resource request for one task submission.

    Parameters
    ----------
    job_name : str
        Human-readable job name.
    cpus : int
        Number of CPUs per task.
    time_limit : str
        Wall-clock time limit (e.g. ``"01:00:00"``).
    mem : str
        Memory request (e.g. ``"4G"``).
    partition : str
        Target partition / queue.
    account : str or None
        Billing account.
    array : str or None
        Array range expression (e.g. ``"0-9%4"``).
    output_path : Path or None
        Standard-output log path.
    error_path : Path or None
        Standard-error log path.
    extra : Dict[str, Any]
        Backend-specific extras (e.g. ``{"gres": "gpu:1"}``).
    """

    job_name: str = ""
    cpus: int = 1
    time_limit: str = "00:30:00"
    mem: str = "4G"
    partition: str = "compute"
    account: Optional[str] = None
    array: Optional[str] = None
    output_path: Optional[Path] = None
    error_path: Optional[Path] = None
    extra: Dict[str, Any] = field(default_factory=dict)


class Executor(abc.ABC):
    """Base class for workload-manager executors.

    Subclasses must implement :meth:`submit` and :meth:`cancel`.
    """

    @abc.abstractmethod
    def submit(
        self,
        resources: JobResources,
        command: List[str],
    ) -> str:
        """Submit a job and return its identifier.

        Parameters
        ----------
        resources : JobResources
            Resource request.
        command : List[str]
            Full command to execute.

        Returns
        -------
        str
            Workload-manager job identifier.
        """

    @abc.abstractmethod
    def cancel(self, job_id: str) -> None:
        """Cancel a submitted or running job.

        Parameters
        ----------
        job_id : str
            Job identifier returned by :meth:`submit`.
        """

    @abc.abstractmethod
    def job_state(self, job_id: str) -> Optional[str]:
        """Query the current state of a job.

        Parameters
        ----------
        job_id : str
            Job identifier.

        Returns
        -------
        str or None
            Backend-specific state string, or ``None`` if unknown.
        """
