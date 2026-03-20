"""Abstract store interface for reflow.

Every storage backend (SQLite, future REST server) implements this
interface so that the orchestration core is backend-agnostic.
"""

from __future__ import annotations

import abc
from typing import Any

from .._types import RunState, TaskState


class Store(abc.ABC):
    """Abstract manifest store."""

    # --- lifecycle ---------------------------------------------------------

    @abc.abstractmethod
    def init(self) -> None:
        """Create tables / schema if they do not exist."""

    @abc.abstractmethod
    def close(self) -> None:
        """Release any held resources."""

    # --- runs --------------------------------------------------------------

    @abc.abstractmethod
    def insert_run(
        self,
        run_id: str,
        graph_name: str,
        user_id: str,
        parameters: dict[str, Any],
    ) -> None:
        """Insert a new run."""

    @abc.abstractmethod
    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Load a single run record."""

    @abc.abstractmethod
    def get_run_parameters(self, run_id: str) -> dict[str, Any]:
        """Load the run parameters.  Raises KeyError if missing."""

    @abc.abstractmethod
    def update_run_status(self, run_id: str, status: RunState) -> None:
        """Update the top-level run status."""

    @abc.abstractmethod
    def list_runs(
        self,
        graph_name: str | None = None,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List runs, optionally filtered."""

    # --- task specs --------------------------------------------------------

    @abc.abstractmethod
    def insert_task_spec(
        self,
        run_id: str,
        task_name: str,
        is_array: bool,
        config_json: dict[str, Any],
        dependencies: list[str],
    ) -> None:
        """Persist one task specification and its dependencies."""

    @abc.abstractmethod
    def list_task_dependencies(self, run_id: str, task_name: str) -> list[str]:
        """Return upstream dependency names for a task."""

    # --- task instances -----------------------------------------------------

    @abc.abstractmethod
    def insert_task_instance(
        self,
        run_id: str,
        task_name: str,
        array_index: int | None,
        state: TaskState,
        input_payload: dict[str, Any],
        identity: str = "",
        input_hash: str = "",
    ) -> int:
        """Insert one task instance, return its database id.

        Parameters
        ----------
        run_id: str
            Uniq identifier for this job.
        task_name: str
            Name of the job.
        identity : str
            Full Merkle identity hash.
        input_hash : str
            Hash of direct inputs only (without upstream hashes).
        array_index: int|None
            Number of array job. If any.
        state: TaskState
            Stats of the task.
        input_payload: dict
            payload of the input jobs
        identity: str
        input_hash: str

        """

    @abc.abstractmethod
    def get_task_instance(
        self,
        run_id: str,
        task_name: str,
        array_index: int | None,
    ) -> dict[str, Any] | None:
        """Load one task instance."""

    @abc.abstractmethod
    def list_task_instances(
        self,
        run_id: str,
        task_name: str | None = None,
        states: list[TaskState] | None = None,
    ) -> list[dict[str, Any]]:
        """List task instances with optional filters."""

    @abc.abstractmethod
    def count_task_instances(self, run_id: str, task_name: str) -> int:
        """Count instances of a task."""

    # --- output retrieval ---------------------------------------------------

    @abc.abstractmethod
    def get_singleton_output(self, run_id: str, task_name: str) -> Any:
        """Load the output of a successful singleton task."""

    @abc.abstractmethod
    def get_array_outputs(self, run_id: str, task_name: str) -> list[Any]:
        """Load all outputs from a successful array task, ordered by index."""

    @abc.abstractmethod
    def get_output_hash(
        self,
        run_id: str,
        task_name: str,
        array_index: int | None = None,
    ) -> str:
        """Get the output hash for a specific successful task instance."""

    @abc.abstractmethod
    def get_all_output_hashes(self, run_id: str, task_name: str) -> list[str]:
        """Get output hashes for all successful instances of a task."""

    # --- dependency check ---------------------------------------------------

    @abc.abstractmethod
    def dependency_is_satisfied(self, run_id: str, task_name: str) -> bool:
        """Check whether all instances of an upstream task succeeded."""

    # --- state transitions --------------------------------------------------

    @abc.abstractmethod
    def update_task_submitted(
        self,
        run_id: str,
        task_name: str,
        job_id: str,
    ) -> None:
        """Mark pending/retrying instances as submitted."""

    @abc.abstractmethod
    def update_task_running(self, instance_id: int) -> None:
        """Mark one instance as running."""

    @abc.abstractmethod
    def update_task_success(
        self,
        instance_id: int,
        output: Any,
        output_hash: str = "",
    ) -> None:
        """Mark one instance as successful.

        Parameters
        ----------
        output_hash : str
            Hash of the output value for downstream Merkle propagation.
        instance_id: int
            Database id
        output: Any
            Output for the job
        output_hash: str
            hash of the output

        """

    @abc.abstractmethod
    def update_task_failed(self, instance_id: int, error_text: str) -> None:
        """Mark one instance as failed."""

    @abc.abstractmethod
    def update_task_cancelled(self, instance_id: int) -> None:
        """Mark one instance as cancelled."""

    @abc.abstractmethod
    def mark_for_retry(self, instance_id: int) -> None:
        """Reset a failed/cancelled instance for retry."""

    # --- cache lookup -------------------------------------------------------

    @abc.abstractmethod
    def find_cached(
        self,
        task_name: str,
        identity: str,
    ) -> dict[str, Any] | None:
        """Find a successful task instance with a matching Merkle identity.

        Searches across *all* runs.  Returns the most recent match.

        Parameters
        ----------
        task_name : str
            Task name.
        identity : str
            Full Merkle identity hash.

        Returns
        -------
        dict[str, Any] or None
            The cached instance row, or ``None`` if no match.

        """

    # --- summary ------------------------------------------------------------

    @abc.abstractmethod
    def task_state_summary(self, run_id: str) -> dict[str, dict[str, int]]:
        """Per-task state counts: ``{task: {state: count}}``."""
