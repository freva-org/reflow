"""File-based worker results for distributed filesystem safety.

Workers write results as JSON files to ``<run_dir>/results/``.
The dispatcher (always single-threaded) ingests them into SQLite.
This eliminates concurrent DB writes from array job workers.

Result files are named ``<task_name>__<array_index>.json`` (or
``<task_name>__singleton.json`` for non-array tasks).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any

from ._types import TaskState
from .manifest import DEFAULT_CODEC
from .stores import Store

logger = logging.getLogger(__name__)


def _results_dir(run_id: str) -> Path:
    """Return the results directory, creating it if needed."""
    cache_dir = Path(os.getenv("XDG_CACHE_HOME") or Path.home() / ".cache" / "reflow")
    cache_dir = cache_dir / "results" / str(run_id)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _result_filename(task_name: str, array_index: int | None) -> str:
    """Generate a result filename."""
    idx = "singleton" if array_index is None else str(array_index)
    return f"{task_name}__{idx}.json"


def write_result(
    run_id: str,
    task_name: str,
    array_index: int | None,
    instance_id: int,
    state: TaskState,
    output: Any = None,
    output_hash: str = "",
    error_text: str = "",
) -> Path:
    """Write a worker result to a JSON file.

    Called by workers instead of writing directly to the store.
    The dispatcher will ingest these files later.

    Parameters
    ----------
    run_id : str
        ID of the running task.
    task_name : str
        Task name.
    array_index : int or None
        Array index, or None for singleton tasks.
    instance_id : int
        Database row id of this task instance.
    state : TaskState
        Final state (SUCCESS or FAILED).
    output : Any
        Task return value (for SUCCESS).
    output_hash : str
        Hash of the output (for Merkle propagation).
    error_text : str
        Traceback text (for FAILED).

    Returns
    -------
    Path
        Path to the written result file.

    """
    d = _results_dir(run_id)
    filename = _result_filename(task_name, array_index)
    path = d / filename

    payload = {
        "task_name": task_name,
        "array_index": array_index,
        "instance_id": instance_id,
        "state": state.value,
        "output_hash": output_hash,
        "error_text": error_text,
    }
    # Output is serialised with the manifest codec for type fidelity.
    if state == TaskState.SUCCESS and output is not None:
        payload["output"] = DEFAULT_CODEC.dump_value(output)
    else:
        payload["output"] = None

    # Write to a temp file first, then rename for atomicity.
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    tmp_path.rename(path)

    logger.debug("Wrote result: %s", path)
    return path


def ingest_results(run_id: str, store: Store) -> int:
    """Read all result files and apply them to the store.

    Called by the dispatcher before the dispatch cycle.  This is
    always single-threaded, so all DB writes are serialised.

    Parameters
    ----------
    run_id : str
        Id of the current run.
    store : Store
        Manifest store.

    Returns
    -------
    int
        Number of results ingested.

    """
    d = _results_dir(run_id)
    count = 0
    for path in sorted(d.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Skipping malformed result file %s: %s", path, exc)
            continue

        instance_id = data.get("instance_id")
        state_str = data.get("state")
        if instance_id is None or state_str is None:
            logger.warning("Skipping incomplete result file %s", path)
            continue

        state = TaskState(state_str)
        if state == TaskState.SUCCESS:
            raw_output = data.get("output")
            output = (
                DEFAULT_CODEC.load_value(raw_output) if raw_output is not None else None
            )
            output_hash = data.get("output_hash", "")
            store.update_task_success(instance_id, output, output_hash=output_hash)
        elif state == TaskState.FAILED:
            error_text = data.get("error_text", "")
            store.update_task_failed(instance_id, error_text)
        else:
            logger.warning("Unexpected state %s in result file %s", state, path)
            continue

        # Remove the result file after successful ingestion.
        try:
            path.unlink()
        except (OSError, PermissionError):
            pass

        count += 1
        logger.debug("Ingested result: %s (state=%s)", path.name, state.value)
    files = [f for f in d.rglob("*.json")]
    if not files:
        try:
            shutil.rmtree(d)
        except (OSError, PermissionError):
            pass
    return count
