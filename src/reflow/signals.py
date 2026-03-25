"""Signal handling for graceful worker shutdown.

When the scheduler sends SIGTERM (or SIGINT via a pre-termination
signal, e.g. Slurm's ``--signal=B:INT@60``), this module converts
the signal into a Python exception so that the worker's existing
try/except block catches it, writes the traceback to the manifest,
and marks the task as FAILED.

Usage::

    with graceful_shutdown():
        result = task_func(**kwargs)

If a signal is received during execution, a [`TaskInterrupted`][TaskInterrupted]
exception is raised.
"""

from __future__ import annotations

import signal
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any


class TaskInterrupted(Exception):
    """Raised when a worker receives a termination signal.

    Attributes
    ----------
    signal_number : int
        The signal that triggered the interruption.
    signal_name : str
        Human-readable signal name.

    """

    def __init__(self, signal_number: int) -> None:
        self.signal_number = signal_number
        self.signal_name = signal.Signals(signal_number).name
        super().__init__(
            f"Task interrupted by {self.signal_name} (signal {signal_number})"
        )


def _signal_handler(signum: int, frame: Any) -> None:
    """Create a signal handler that raises TaskInterrupted."""
    raise TaskInterrupted(signum)


@contextmanager
def graceful_shutdown() -> Generator[None, None, None]:
    """Context manager that converts SIGTERM/SIGINT to TaskInterrupted.

    Restores the original signal handlers on exit.

    Examples
    --------
    >>> with graceful_shutdown():
    ...     result = expensive_computation()

    """
    old_sigterm = signal.getsignal(signal.SIGTERM)
    old_sigint = signal.getsignal(signal.SIGINT)

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, old_sigterm)
        signal.signal(signal.SIGINT, old_sigint)
