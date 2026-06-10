"""Utilities for executing flows."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass


@dataclass
class CommandResult:
    """Result of a completed subprocess invocation.

    Attributes:
        cmd: The command and its arguments as passed to the subprocess.
        stdout: Captured standard output, stripped of leading/trailing whitespace.
        stderr: Captured standard error, stripped of leading/trailing whitespace.
            Non-empty stderr does not imply failure — many tools write warnings
            or progress info there even on success. Check ``returncode`` or catch
            ``ExecutorError`` for failure detection.
        returncode: The process exit code. Zero indicates success by Unix convention.

    """

    cmd: list[str]
    stdout: str
    stderr: str
    returncode: int


class ExecutorError(RuntimeError):
    """Raised when a subprocess exits with a non-zero return code."""

    def __init__(self, result: CommandResult) -> None:
        self.result = result
        super().__init__(
            f"Command {result.cmd!r} failed (exit {result.returncode}):\n{result.stderr}"
        )


def run_cmd(
    cmd: list[str],
    *,
    timeout: float | None = 60.0,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    check: bool = True,
) -> CommandResult:
    """Run a subprocess and return a structured result.

    Raises ExecutorError on non-zero exit, always capturing both streams.
    Stderr from successful runs is preserved in the result for callers
    that want to log or inspect it.
    """
    proc = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        timeout=timeout,
        env=env,
        cwd=cwd,
    )
    result = CommandResult(
        cmd=cmd,
        stdout=proc.stdout.strip(),
        stderr=proc.stderr.strip(),
        returncode=proc.returncode,
    )
    if proc.returncode != 0 and check:
        raise ExecutorError(result)
    return result
