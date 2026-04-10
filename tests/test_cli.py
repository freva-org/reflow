"""test_cli.py - refactored reflow tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Literal
from unittest.mock import patch

import pytest

from reflow import (
    Param,
    Result,
    Run,
    RunDir,
    Workflow,
)
from reflow.stores.sqlite import SqliteStore
from reflow.cli import build_parser, parse_args, run_command

# ═══════════════════════════════════════════════════════════════════════════
# Coverage: _dispatch.py  (dispatch loop, resolve, fan-out, finalize)
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════


class TestCLICommands:
    @staticmethod
    def _make_wf() -> Workflow:
        wf = Workflow("cli_test")

        @wf.job()
        def task_a(
            x: Annotated[str, Param(help="X")],
            run_dir: RunDir = RunDir(),
        ) -> str:
            return "ok"

        return wf

    def test_runs_command(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from reflow.cli import parse_args, run_command

        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._make_wf()
        # First submit so there's a run
        run = wf.submit(run_dir=tmp_path / "r", x="val")

        args = parse_args(wf, ["runs"])
        rc = run_command(wf, args)
        assert rc == 0

    def test_status_command(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from reflow.cli import parse_args, run_command

        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._make_wf()
        run = wf.submit(run_dir=tmp_path / "r", x="val")

        args = parse_args(wf, ["status", run.run_id])
        rc = run_command(wf, args)
        assert rc == 0
        out = capsys.readouterr().out
        assert run.run_id in out

    def test_cancel_command(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from reflow.cli import parse_args, run_command

        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._make_wf()
        run = wf.submit(run_dir=tmp_path / "r", x="val")

        args = parse_args(wf, ["cancel", run.run_id])
        rc = run_command(wf, args)
        assert rc == 0

    def test_submit_command(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from reflow.cli import parse_args, run_command

        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._make_wf()

        args = parse_args(
            wf,
            ["submit", "--run-dir", str(tmp_path / "r"), "--x", "val"],
        )
        rc = run_command(wf, args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "Created run" in out or "cli_test" in out


class TestCLIForceFlags:
    @staticmethod
    def _make_wf() -> Workflow:
        wf = Workflow("force_test")

        @wf.job()
        def task_a(
            x: Annotated[str, Param(help="X")],
            run_dir: RunDir = RunDir(),
        ) -> str:
            return "ok"

        return wf

    def test_force_flag_parses(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        args = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v", "--force"],
        )
        assert args.force is True

    def test_force_tasks_flag_parses(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        args = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v",
             "--force-tasks", "task_a"],
        )
        assert args.force_tasks == ["task_a"]

    def test_force_tasks_multiple(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        args = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v",
             "--force-tasks", "task_a", "task_b"],
        )
        assert args.force_tasks == ["task_a", "task_b"]

    def test_defaults_without_flags(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        args = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v"],
        )
        assert args.force is False
        assert args.force_tasks is None

    def test_force_stored_in_parameters(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from reflow.cli import parse_args, run_command

        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._make_wf()
        store_path = str(tmp_path / "db.sqlite")

        args = parse_args(
            wf,
            ["submit", "--run-dir", str(tmp_path / "r"), "--x", "v",
             "--force", "--store-path", store_path],
        )
        run_command(wf, args)

        st = SqliteStore(store_path)
        st.init()
        runs = st.list_runs(graph_name="force_test")
        params = st.get_run_parameters(runs[-1]["run_id"])
        assert params.get("__force__") is True
        # x should be a normal parameter, not contaminated by force
        assert params.get("x") == "v"

    def test_force_tasks_stored_in_parameters(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from reflow.cli import parse_args, run_command

        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._make_wf()
        store_path = str(tmp_path / "db.sqlite")

        args = parse_args(
            wf,
            ["submit", "--run-dir", str(tmp_path / "r"), "--x", "v",
             "--force-tasks", "task_a", "--store-path", store_path],
        )
        run_command(wf, args)

        st = SqliteStore(store_path)
        st.init()
        runs = st.list_runs(graph_name="force_test")
        params = st.get_run_parameters(runs[-1]["run_id"])
        assert params.get("__force_tasks__") == ["task_a"]
        assert "__force__" not in params

    def test_force_flag_in_help(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        # parse_args with ["submit", "--help"] would SystemExit,
        # so just verify the flag is accepted by parsing it.
        args = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v", "--force"],
        )
        assert args.force is True
        # And without it, it defaults to False (not an unknown arg)
        args2 = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v"],
        )
        assert args2.force is False

    def test_force_not_leaked_as_task_param(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """--force and --force-tasks should not appear as task parameters."""
        from reflow.cli import parse_args, run_command

        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._make_wf()
        store_path = str(tmp_path / "db.sqlite")

        args = parse_args(
            wf,
            ["submit", "--run-dir", str(tmp_path / "r"), "--x", "v",
             "--force", "--force-tasks", "task_a",
             "--store-path", store_path],
        )
        run_command(wf, args)

        st = SqliteStore(store_path)
        st.init()
        runs = st.list_runs(graph_name="force_test")
        params = st.get_run_parameters(runs[-1]["run_id"])
        # These should not be present as regular parameters
        assert "force" not in params
        assert "force_tasks" not in params


class TestCLI:
    @staticmethod
    def _make_wf() -> Workflow:
        wf = Workflow("cli_test")

        @wf.job()
        def prepare(
            start: Annotated[str, Param(help="Start")],
            model: Annotated[Literal["era5", "icon"], Param(help="Model")] = "era5",
            run_dir: RunDir = RunDir(),
        ) -> list[str]:
            return []

        @wf.array_job()
        def convert(
            item: Annotated[str, Result(step="prepare")],
            bucket: Annotated[str, Param(help="B")],
        ) -> str:
            return item

        return wf

    def test_parser_builds(self) -> None:
        from reflow.cli import build_parser

        assert build_parser(self._make_wf()).prog == "cli_test"

    def test_submit_parses(self) -> None:
        from reflow.cli import build_parser

        args = build_parser(self._make_wf()).parse_args(
            [
                "submit",
                "--run-dir",
                "/tmp",
                "--start",
                "2025-01-01",
                "--bucket",
                "b",
                "--model",
                "icon",
            ]
        )
        assert args.start == "2025-01-01"
        assert args.model == "icon"

    def test_literal_choices_enforced(self) -> None:
        from reflow.cli import build_parser

        with pytest.raises(SystemExit):
            build_parser(self._make_wf()).parse_args(
                [
                    "submit",
                    "--run-dir",
                    "/tmp",
                    "--start",
                    "x",
                    "--bucket",
                    "b",
                    "--model",
                    "invalid",
                ]
            )

    def test_hidden_commands(self) -> None:
        from reflow.cli import build_parser, parse_args

        wf = self._make_wf()
        assert "dispatch" not in build_parser(wf).format_help()
        args = parse_args(wf, ["dispatch", "--run-id", "x", "--run-dir", "/tmp"])
        assert args._command == "dispatch"

    def test_describe_command(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        args = parse_args(wf, ["describe"])
        assert args._command == "describe"


class TestCLIExtended:
    @staticmethod
    def _make_wf() -> Workflow:
        wf = Workflow("cli_ext")

        @wf.job()
        def task_a(
            x: Annotated[str, Param(help="X")],
            run_dir: RunDir = RunDir(),
        ) -> str:
            return "ok"

        return wf

    def test_dag_command(self, capsys: pytest.CaptureFixture[str]) -> None:
        from reflow.cli import parse_args, run_command

        wf = self._make_wf()
        args = parse_args(wf, ["dag"])
        rc = run_command(wf, args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "task_a" in out

    def test_describe_command(self, capsys: pytest.CaptureFixture[str]) -> None:
        from reflow.cli import parse_args, run_command

        wf = self._make_wf()
        args = parse_args(wf, ["describe"])
        rc = run_command(wf, args)
        assert rc == 0
        out = capsys.readouterr().out
        manifest = json.loads(out)
        assert manifest["name"] == "cli_ext"

    def test_worker_parser_builds(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        args = parse_args(
            wf,
            [
                "worker",
                "--run-id",
                "r1",
                "--run-dir",
                "/tmp",
                "--task",
                "task_a",
            ],
        )
        assert args._command == "worker"
        assert args.task == "task_a"

    def test_worker_parser_with_index(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        args = parse_args(
            wf,
            [
                "worker",
                "--run-id",
                "r1",
                "--run-dir",
                "/tmp",
                "--task",
                "t",
                "--index",
                "3",
            ],
        )
        assert args.index == 3

    def test_dispatch_parser_with_verify(self) -> None:
        from reflow.cli import parse_args

        wf = self._make_wf()
        args = parse_args(
            wf,
            [
                "dispatch",
                "--run-id",
                "r1",
                "--run-dir",
                "/tmp",
                "--verify",
            ],
        )
        assert args.verify is True
