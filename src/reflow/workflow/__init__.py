"""Runnable workflow (extends Flow with execution machinery).

[`Workflow`][reflow.Workflow] adds validation, submission, dispatch, worker
execution, cancellation, retry, and status queries to [`Flow`][Flow].

This package splits the implementation across submodules for
maintainability while preserving the original ``reflow.workflow``
import surface:

- ``_core``     — :class:`Workflow` (submit, validate, cancel, retry,
  status, describe, resource helpers)
- ``_dispatch`` — :class:`DispatchMixin` (dispatch loop, cache, result
  wiring, fan-out)
- ``_worker``   — :class:`WorkerMixin` (task execution)
- ``_helpers``  — module-level functions (``_build_kwargs``,
  ``_make_run_id``, ``_resolve_executor``, ``_resolve_index``)
"""

from ._core import Workflow
from ._helpers import _build_kwargs, _make_run_id, _resolve_executor, _resolve_index

__all__ = [
    "Workflow",
    "_build_kwargs",
    "_make_run_id",
    "_resolve_executor",
    "_resolve_index",
]
