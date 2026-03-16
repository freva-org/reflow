"""Shared type definitions for slurm_flow."""

from __future__ import annotations

import enum


class TaskState(str, enum.Enum):
    """Lifecycle state of a task instance.

    Attributes
    ----------
    PENDING : str
        Task registered but not yet submitted.
    SUBMITTED : str
        Submitted to the workload manager.
    RUNNING : str
        Currently executing.
    SUCCESS : str
        Completed successfully.
    FAILED : str
        Completed with an error.
    CANCELLED : str
        Cancelled by the user or the system.
    RETRYING : str
        Marked for re-execution after a previous failure.
    """

    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RETRYING = "RETRYING"

    # -- convenience sets for query filters --------------------------------

    @classmethod
    def terminal(cls) -> frozenset["TaskState"]:
        """Return terminal states that will not change without intervention.

        Returns
        -------
        frozenset of TaskState
        """
        return frozenset({cls.SUCCESS, cls.FAILED, cls.CANCELLED})

    @classmethod
    def active(cls) -> frozenset["TaskState"]:
        """Return states that represent in-progress work.

        Returns
        -------
        frozenset of TaskState
        """
        return frozenset({cls.PENDING, cls.SUBMITTED, cls.RUNNING, cls.RETRYING})

    @classmethod
    def cancellable(cls) -> frozenset["TaskState"]:
        """Return states from which cancellation is meaningful.

        Returns
        -------
        frozenset of TaskState
        """
        return frozenset({cls.PENDING, cls.SUBMITTED, cls.RUNNING})

    @classmethod
    def retriable(cls) -> frozenset["TaskState"]:
        """Return states from which a retry can be triggered.

        Returns
        -------
        frozenset of TaskState
        """
        return frozenset({cls.FAILED, cls.CANCELLED})


class RunState(str, enum.Enum):
    """Top-level state of a DAG run.

    Attributes
    ----------
    RUNNING : str
        At least one task is still active.
    SUCCESS : str
        All tasks finished successfully.
    FAILED : str
        At least one task failed and no active tasks remain.
    CANCELLED : str
        Run was explicitly cancelled.
    """

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
