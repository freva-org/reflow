"""Merkle-DAG caching for reflow.

Each task instance gets an *identity* hash computed from:

- task name
- task version (user-controllable)
- direct inputs: the upstream values this instance receives
- CLI / run parameters the task function actually takes (``Param``
  values, task-local overrides); ``RunDir`` is excluded so identical
  work in different run directories still hits the cache
- upstream output hashes (propagated through the DAG)

If a previous successful instance with the same identity exists in
the store, its output is reused without submitting a scheduler job.

For tasks returning ``Path`` or ``list[Path]``, the cached output is
additionally verified by checking that the files still exist on disk.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, get_args, get_origin


def compute_input_hash(
    task_name: str,
    version: str,
    direct_inputs: dict[str, Any],
    parameters: dict[str, Any] | None = None,
) -> str:
    """Hash a task's name, version, direct inputs and parameters.

    Parameters
    ----------
    task_name : str
        Task name.
    version : str
        User-controllable version string.
    direct_inputs : dict[str, Any]
        Upstream (``Result``) values for this instance.
    parameters : dict[str, Any] or None
        Run parameters the task function receives (see
        `reflow.workflow._helpers.parameter_inputs`).  Omitted from the
        hash when empty, so tasks without parameters keep the identity
        they had before parameters were hashed.

    Returns
    -------
    str
        Hex digest (sha256, truncated to 16 chars).

    """
    payload: dict[str, Any] = {
        "task": task_name,
        "version": version,
        "inputs": direct_inputs,
    }
    if parameters:
        payload["params"] = parameters
    blob = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def compute_identity(
    input_hash: str,
    upstream_output_hashes: list[str],
) -> str:
    """Compute the full Merkle identity from input hash + upstream hashes.

    Parameters
    ----------
    input_hash : str
        Hash of this task's direct inputs (from :func:`compute_input_hash`).
    upstream_output_hashes : list[str]
        Sorted output hashes from upstream dependencies.

    Returns
    -------
    str
        Hex digest (sha256, truncated to 16 chars).

    """
    blob = (input_hash + "|" + "|".join(sorted(upstream_output_hashes))).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def compute_output_hash(output: Any) -> str:
    """Hash a task's output for downstream identity propagation.

    Parameters
    ----------
    output : Any
        The task's return value (must be JSON-serialisable).

    Returns
    -------
    str
        Hex digest (sha256, truncated to 16 chars).

    """
    blob = json.dumps(output, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def verify_cached_output(
    output: Any,
    return_type: Any,
    verify: Callable[[Any], bool] | None,
) -> bool:
    """Check whether a cached output is still valid.

    Resolution order:

    1. If *verify* is a callable, call it with *output* and return
       the result.
    2. If *return_type* is ``Path``, check that the path exists.
    3. If *return_type* is ``list[Path]``, check that all paths exist.
    4. Otherwise, return ``True`` (trust the cache).

    Parameters
    ----------
    output : Any
        The cached output value.
    return_type : Any
        The task's return type annotation.
    verify : callable or None
        Optional user-supplied verification function.

    Returns
    -------
    bool
        ``True`` if the cached output is still valid.

    """
    # User-supplied callable takes priority.
    if verify is not None:
        return bool(verify(output))

    # Auto-detect: Path or list[Path].
    if return_type is Path:
        try:
            return Path(output).exists()
        except (TypeError, ValueError):
            return True

    if get_origin(return_type) is list:
        args = get_args(return_type)
        if args and args[0] is Path:
            try:
                return all(Path(p).exists() for p in output)
            except (TypeError, ValueError):
                return True

    # Non-path types: trust the identity hash.
    return True
