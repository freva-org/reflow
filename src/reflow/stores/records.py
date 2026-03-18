"""Typed internal record models for storage backends.

These dataclasses model persisted store rows after decoding from the backend.
They are intentionally dependency-free and keep backend code away from ad-hoc
row dictionaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .._types import RunState, TaskState


@dataclass(frozen=True)
class RunRecord:
    """Typed representation of one workflow run row."""

    run_id: str
    graph_name: str
    user_id: str
    created_at: datetime
    status: RunState
    parameters: dict[str, Any]

    def to_public_dict(self) -> dict[str, Any]:
        """Return the legacy public dictionary shape."""
        return {
            "run_id": self.run_id,
            "graph_name": self.graph_name,
            "user_id": self.user_id,
            "created_at": self.created_at.isoformat(),
            "status": self.status.value,
            "parameters": self.parameters,
        }


@dataclass(frozen=True)
class TaskSpecRecord:
    """Typed representation of one persisted task specification."""

    run_id: str
    task_name: str
    is_array: bool
    config: dict[str, Any]
    dependencies: list[str]


@dataclass(frozen=True)
class TaskInstanceRecord:
    """Typed representation of one persisted task instance."""

    id: int
    run_id: str
    task_name: str
    array_index: int | None
    state: TaskState
    job_id: str | None
    input: dict[str, Any]
    output: Any
    error_text: str | None
    identity: str
    input_hash: str
    output_hash: str
    created_at: datetime
    updated_at: datetime

    def to_public_dict(self) -> dict[str, Any]:
        """Return the legacy public dictionary shape."""
        return {
            "id": self.id,
            "run_id": self.run_id,
            "task_name": self.task_name,
            "array_index": self.array_index,
            "state": self.state.value,
            "job_id": self.job_id,
            "input": self.input,
            "output": self.output,
            "error_text": self.error_text,
            "identity": self.identity,
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }
