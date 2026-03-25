"""Worker mixin for the Workflow class.

Contains the :meth:`worker` method that executes a single task
instance inside a scheduler job (or local subprocess).
"""

from __future__ import annotations

import json
import logging
import traceback
from pathlib import Path
from typing import Any

from .._types import TaskState
from ..cache import compute_output_hash
from ..executors import Executor
from ..results import write_result
from ..signals import graceful_shutdown
from ..stores import Store
from ._helpers import build_kwargs, resolve_index

logger = logging.getLogger(__name__)


class WorkerMixin:
    """Worker execution logic, mixed into :class:`~reflow.workflow.Workflow`."""

    def worker(
        self,
        run_id: str,
        store: Store,
        run_dir: Path,
        task_name: str,
        index: int | None = None,
        executor: Executor | None = None,
    ) -> None:
        """Execute one task instance and write a result file.

        Workers only *read* from the store (input payload, parameters).
        Results are written as JSON files to ``<run_dir>/results/``
        and ingested by the dispatcher.  This avoids concurrent SQLite
        writes from array job workers on distributed filesystems.

        Signal handling: SIGTERM and SIGINT are converted to
        :class:`~reflow.signals.TaskInterrupted` so that the error
        handler writes a FAILED result file with the traceback.
        """
        if task_name not in self.tasks:  # type: ignore[attr-defined]
            raise KeyError(f"Unknown task: {task_name!r}")

        resolved_index = resolve_index(index)
        spec = self.tasks[task_name]  # type: ignore[attr-defined]

        row = store.get_task_instance(run_id, task_name, resolved_index)
        if row is None:
            raise KeyError(
                f"Instance not found: {run_id!r}/{task_name!r}/{resolved_index!r}"
            )
        parameters = store.get_run_parameters(run_id)
        task_input: Any = row.get("input", {})
        if isinstance(task_input, str):
            task_input = json.loads(task_input)
        kwargs = build_kwargs(spec, parameters, task_input or {})

        instance_id = int(row["id"])

        # Mark as running in the DB (best-effort; retry on lock).
        try:
            store.update_task_running(instance_id)
        except Exception:
            logger.warning(
                "Could not mark %s[%s] as RUNNING in DB",
                task_name,
                resolved_index,
            )

        try:
            with graceful_shutdown():
                result = spec.func(**kwargs)
            out_hash = compute_output_hash(result)
            write_result(
                run_id=run_id,
                task_name=task_name,
                array_index=resolved_index,
                instance_id=instance_id,
                state=TaskState.SUCCESS,
                output=result,
                output_hash=out_hash,
            )
        except Exception:
            write_result(
                run_id=run_id,
                task_name=task_name,
                array_index=resolved_index,
                instance_id=instance_id,
                state=TaskState.FAILED,
                error_text=traceback.format_exc(),
            )
            raise
