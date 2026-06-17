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
    RunDir,
    Workflow,
)
from reflow.cli import build_parser, parse_args, run_command
from reflow.stores.sqlite import SqliteStore

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

        wf = self._make_wf()
        args = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v", "--force"],
        )
        assert args.force is True

    def test_force_tasks_flag_parses(self) -> None:

        wf = self._make_wf()
        args = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v",
             "--force-tasks", "task_a"],
        )
        assert args.force_tasks == ["task_a"]

    def test_force_tasks_multiple(self) -> None:

        wf = self._make_wf()
        args = parse_args(
            wf,
            ["submit", "--run-dir", "/tmp/r", "--x", "v",
             "--force-tasks", "task_a", "task_b"],
        )
        assert args.force_tasks == ["task_a", "task_b"]

    def test_defaults_without_flags(self) -> None:

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

        assert build_parser(self._make_wf()).prog == "cli_test"

    def test_submit_parses(self) -> None:

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

        wf = self._make_wf()
        assert "dispatch" not in build_parser(wf).format_help()
        args = parse_args(wf, ["dispatch", "--run-id", "x", "--run-dir", "/tmp"])
        assert args._command == "dispatch"

    def test_describe_command(self) -> None:

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

        wf = self._make_wf()
        args = parse_args(wf, ["dag"])
        rc = run_command(wf, args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "task_a" in out

    def test_describe_command(self, capsys: pytest.CaptureFixture[str]) -> None:

        wf = self._make_wf()
        args = parse_args(wf, ["describe"])
        rc = run_command(wf, args)
        assert rc == 0
        out = capsys.readouterr().out
        manifest = json.loads(out)
        assert manifest["name"] == "cli_ext"

    def test_worker_parser_builds(self) -> None:

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


# ═══════════════════════════════════════════════════════════════════════════
# CLI: _resolve_run_dir, task-local params, retry/dispatch/worker commands
# ═══════════════════════════════════════════════════════════════════════════


class TestResolveRunDir:
    def _make_wf(self) -> Workflow:

        from reflow import Workflow

        wf = Workflow("wf")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        return wf

    def test_resolve_from_args_run_dir(self, tmp_path: Path) -> None:
        from unittest.mock import MagicMock

        from reflow.cli import _resolve_run_dir

        args = MagicMock()
        args.run_dir = str(tmp_path)
        result = _resolve_run_dir(args, None)
        assert result == tmp_path

    def test_resolve_from_store_params(self, tmp_path: Path) -> None:
        from unittest.mock import MagicMock

        from reflow.cli import _resolve_run_dir
        from reflow.stores.sqlite import SqliteStore

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        st.insert_run("r1", "wf", "u", {"run_dir": str(tmp_path)})

        args = MagicMock()
        args.run_dir = None
        args.run_id = "r1"
        result = _resolve_run_dir(args, st)
        assert result == tmp_path
        st.close()

    def test_resolve_no_run_id_raises(self, tmp_path: Path) -> None:
        from unittest.mock import MagicMock

        from reflow.cli import _resolve_run_dir

        args = MagicMock()
        args.run_dir = None
        args.run_id = None
        with pytest.raises(ValueError, match="run_dir is required"):
            _resolve_run_dir(args, None)

    def test_resolve_missing_run_dir_in_store_raises(self, tmp_path: Path) -> None:
        from unittest.mock import MagicMock

        from reflow.cli import _resolve_run_dir
        from reflow.stores.sqlite import SqliteStore

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        st.insert_run("r1", "wf", "u", {})  # no run_dir in params

        args = MagicMock()
        args.run_dir = None
        args.run_id = "r1"
        with pytest.raises(KeyError):
            _resolve_run_dir(args, st)
        st.close()


class TestCLIRunCommands:
    def _wf(self) -> Workflow:

        from reflow import Workflow

        wf = Workflow("wf")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        return wf

    def test_retry_command(self, tmp_path: Path) -> None:
        from reflow.cli import parse_args, run_command
        from reflow.executors.util import CommandResult
        from reflow.stores.sqlite import SqliteStore

        wf = self._wf()
        store_path = str(tmp_path / "db.sqlite")
        with SqliteStore(store_path) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st, x="v")
            run_id = run.run_id
            row = st.get_task_instance(run_id, "task", None)
            st.update_task_failed(int(row["id"]), "simulated error")

        # retry calls _submit_dispatch → sbatch; mock run_cmd to avoid it
        fake_result = CommandResult(cmd=[], stdout="dry-run-job-1", stderr="", returncode=0)
        with patch("reflow.executors.slurm.run_cmd", return_value=fake_result):
            args = parse_args(wf, ["retry", run_id, "--store-path", store_path])
            rc = run_command(wf, args)
        assert rc == 0

    def test_dispatch_command(self, tmp_path: Path) -> None:
        from reflow.stores.sqlite import SqliteStore

        wf = self._wf()
        store_path = str(tmp_path / "db.sqlite")
        with SqliteStore(store_path) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st, x="v")
            run_id = run.run_id

        # dispatch is a hidden command; uses --run-id, resolves run_dir from store
        args = parse_args(wf, [
            "dispatch",
            "--run-id", run_id,
            "--store-path", store_path,
        ])
        rc = run_command(wf, args)
        assert rc == 0

    def test_worker_command(self, tmp_path: Path) -> None:
        from reflow.stores.sqlite import SqliteStore

        wf = self._wf()
        store_path = str(tmp_path / "db.sqlite")
        with SqliteStore(store_path) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st, x="hi")
            run_id = run.run_id

        args = parse_args(wf, [
            "worker",
            "--run-id", run_id,
            "--run-dir", str(tmp_path / "r"),
            "--task", "task",
            "--store-path", store_path,
        ])
        rc = run_command(wf, args)
        assert rc == 0

    def test_runs_command_empty(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:

        wf = self._wf()
        store_path = str(tmp_path / "db.sqlite")
        args = parse_args(wf, ["runs", "--store-path", store_path])
        rc = run_command(wf, args)
        out = capsys.readouterr().out
        assert rc == 0
        assert "No runs found" in out


class TestCLITaskLocalParams:
    def test_task_local_params_stored(self, tmp_path: Path) -> None:
        """Task-local params (--task.param) are stored under __task_params__."""
        from reflow import Config, Workflow
        from reflow.stores.sqlite import SqliteStore

        # dry-run so submit_run does not call sbatch
        wf = Workflow("wf", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def convert(
            chunk_size: Annotated[int, Param(help="chunk", namespace="local")],
        ) -> str:
            return str(chunk_size)

        store_path = str(tmp_path / "db.sqlite")
        args = parse_args(wf, [
            "submit",
            "--run-dir", str(tmp_path / "r"),
            "--store-path", store_path,
            "--convert-chunk-size", "256",
        ])
        run_command(wf, args)
        with SqliteStore(store_path) as st:
            runs = st.list_runs()
            assert runs
            params = st.get_run_parameters(runs[0]["run_id"])
        assert "__task_params__" in params
        assert params["__task_params__"]["convert"]["chunk_size"] == 256


class TestCLIRichImportFallback:
    def test_arg_formatter_fallback(self) -> None:
        """When rich_argparse is unavailable the plain HelpFormatter is used."""
        import importlib
        import sys

        with patch.dict(sys.modules, {"rich_argparse": None}):
            import reflow.cli as cli_mod
            importlib.reload(cli_mod)
            assert cli_mod.ArgFormatter is not None
        importlib.reload(cli_mod)  # restore


class TestCLIStatusTaskFilter:
    def test_status_with_task_filter_shows_instances(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:

        from reflow import Workflow
        from reflow.stores.sqlite import SqliteStore

        wf = Workflow("wf")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        store_path = str(tmp_path / "db.sqlite")
        with SqliteStore(store_path) as st:
            run = wf.run_local(run_dir=tmp_path / "r", store=st, x="hello")
            run_id = run.run_id

        args = parse_args(wf, [
            "status", run_id,
            "--store-path", store_path,
            "--task", "task",
        ])
        rc = run_command(wf, args)
        out = capsys.readouterr().out
        assert rc == 0
        assert "task" in out


class TestCLICancelAbort:
    def test_cancel_multi_run_eof_aborts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]
    ) -> None:

        from reflow import Workflow
        from reflow.stores.sqlite import SqliteStore

        wf = Workflow("wf")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        store_path = str(tmp_path / "db.sqlite")
        with SqliteStore(store_path) as st:
            run1 = wf.run_local(run_dir=tmp_path / "r1", store=st, x="a")
            run2 = wf.run_local(run_dir=tmp_path / "r2", store=st, x="b")

        monkeypatch.setattr("builtins.input", lambda _: (_ for _ in ()).throw(EOFError()))
        args = parse_args(wf, [
            "cancel", run1.run_id, run2.run_id,
            "--store-path", store_path,
        ])
        rc = run_command(wf, args)
        assert rc == 1
        assert "Aborted" in capsys.readouterr().out


class TestCLIMain:
    def test_main_version_flag(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from reflow.cli import main
        monkeypatch.setattr("sys.argv", ["reflow", "--version"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
        assert capsys.readouterr().out.strip().startswith("reflow")

    def test_main_no_args_prints_message(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from reflow.cli import main
        monkeypatch.setattr("sys.argv", ["reflow"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
        assert "not yet implemented" in capsys.readouterr().out

    def test_parse_args_defaults_to_sys_argv(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        from reflow import Workflow
        wf = Workflow("wf")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        monkeypatch.setattr("sys.argv", ["wf", "dag"])
        args = parse_args(wf, None)
        assert args._command == "dag"


class TestCLIMethod:
    def test_workflow_cli_method(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:

        from reflow import Workflow
        wf = Workflow("wf")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        rc = wf.cli(["dag"])
        out = capsys.readouterr().out
        assert rc == 0
        assert "task" in out or "wf" in out
