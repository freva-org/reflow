"""test_misc.py - refactored reflow tests."""

from __future__ import annotations

import signal as sig
from pathlib import Path

import pytest

from reflow import (
    Run,
    RunDir,
    Workflow,
)
from reflow.cli import build_parser
from reflow.signals import TaskInterrupted, graceful_shutdown

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════


class TestSignals:
    def test_task_interrupted(self) -> None:
        import signal as sig

        from reflow.signals import TaskInterrupted

        exc = TaskInterrupted(sig.SIGTERM)
        assert exc.signal_number == sig.SIGTERM
        assert "SIGTERM" in str(exc)
        assert "SIGTERM" in exc.signal_name

    def test_graceful_shutdown_normal(self) -> None:
        from reflow.signals import graceful_shutdown

        # Should not raise if no signal is sent.
        with graceful_shutdown():
            x = 1 + 1
        assert x == 2

    def test_graceful_shutdown_restores_handlers(self) -> None:
        import signal as sig

        from reflow.signals import graceful_shutdown

        old_term = sig.getsignal(sig.SIGTERM)
        old_int = sig.getsignal(sig.SIGINT)
        with graceful_shutdown():
            pass
        assert sig.getsignal(sig.SIGTERM) is old_term
        assert sig.getsignal(sig.SIGINT) is old_int


class TestSignalsExtended:
    def test_task_interrupted_attributes(self) -> None:
        from reflow.signals import TaskInterrupted

        exc = TaskInterrupted(sig.SIGINT)
        assert exc.signal_number == sig.SIGINT
        assert exc.signal_name == "SIGINT"
        assert "SIGINT" in str(exc)


class TestVersion:
    def test_version_exists(self) -> None:
        import reflow

        assert hasattr(reflow, "__version__")

    def test_cli_version(self) -> None:
        from reflow.cli import build_parser

        wf = Workflow("test")

        @wf.job()
        def a(run_dir: RunDir = RunDir()) -> str:
            return "ok"

        parser = build_parser(wf)
        # --version should be a recognised option
        help_text = parser.format_help()
        assert "--version" in help_text
