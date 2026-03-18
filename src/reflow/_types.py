"""Shared state enums for reflow."""

from __future__ import annotations

import enum


class TaskState(str, enum.Enum):
    """Lifecycle state of a task instance."""

    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RETRYING = "RETRYING"

    @classmethod
    def terminal(cls) -> frozenset[TaskState]:
        """States that will not change without intervention."""
        return frozenset({cls.SUCCESS, cls.FAILED, cls.CANCELLED})

    @classmethod
    def active(cls) -> frozenset[TaskState]:
        """States that represent in-progress work."""
        return frozenset({cls.PENDING, cls.SUBMITTED, cls.RUNNING, cls.RETRYING})

    @classmethod
    def cancellable(cls) -> frozenset[TaskState]:
        """States from which cancellation is meaningful."""
        return frozenset({cls.PENDING, cls.SUBMITTED, cls.RUNNING})

    @classmethod
    def retriable(cls) -> frozenset[TaskState]:
        """States from which a retry can be triggered."""
        return frozenset({cls.FAILED, cls.CANCELLED})


class RunState(str, enum.Enum):
    """Top-level state of a workflow run."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
