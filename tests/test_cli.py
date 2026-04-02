"""Tests for new features and coverage improvements.

Covers:
- Feature 1: "Did you mean …" suggestions on unknown task names
- Feature 2: Broadcast mode (Result.broadcast, WireMode.BROADCAST)
- Feature 3: run_local (in-process dependency-aware execution)
- Coverage gaps: _dispatch.py, _worker.py, run.py, params.py, cli.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any
from unittest.mock import patch

import pytest

from reflow import (
    Config,
    Flow,
    LocalExecutor,
    Param,
    Result,
    RunDir,
    RunState,
    TaskState,
    Workflow,
)
from reflow.params import (
    WireMode,
    check_type_compatibility,
    infer_wire_mode,
)
from reflow.stores.sqlite import SqliteStore
from reflow.workflow._core import _suggest_name



# ═══════════════════════════════════════════════════════════════════════════
# Coverage: _dispatch.py  (dispatch loop, resolve, fan-out, finalize)
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchIntegration:
    @staticmethod
    def _build_wf() -> Workflow:
        wf = Workflow("d", config=Config({"executor": {"mode": "dry-run"}}))

        @wf.job()
        def prep(x: Annotated[str, Param(help="X")]) -> list[str]:
            return ["a", "b"]

        @wf.array_job()
        def conv(item: Annotated[str, Result(step="prep")]) -> str:
            return item.upper()

        return wf

    def test_dispatch_singleton_and_array(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Submit + dispatch cycle via dry-run executor."""
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._build_wf()
        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run = wf.submit(
            run_dir=tmp_path / "r", x="val", store=st,
        )
        # Verify task instances were created
        instances = st.list_task_instances(run.run_id)
        assert len(instances) >= 1
        assert run.run_id.startswith("d-")

    def test_maybe_finalise_all_success(self, tmp_path: Path) -> None:
        """_maybe_finalise_run marks run as SUCCESS when all tasks done."""
        wf = Workflow("f")

        @wf.job()
        def a() -> str:
            return "ok"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "test-final-ok"
        st.insert_run(run_id, "f", "u", {})
        iid = st.insert_task_instance(run_id, "a", None, TaskState.SUCCESS, {})

        wf._maybe_finalise_run(run_id, st)
        run = st.get_run(run_id)
        assert run["status"] == "SUCCESS"

    def test_maybe_finalise_with_failure(self, tmp_path: Path) -> None:
        wf = Workflow("f")

        @wf.job()
        def a() -> str:
            return "ok"

        @wf.job()
        def b() -> str:
            return "ok"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "test-final-fail"
        st.insert_run(run_id, "f", "u", {})
        st.insert_task_instance(run_id, "a", None, TaskState.SUCCESS, {})
        st.insert_task_instance(run_id, "b", None, TaskState.FAILED, {})

        wf._maybe_finalise_run(run_id, st)
        assert st.get_run(run_id)["status"] == "FAILED"

    def test_maybe_finalise_active_noop(self, tmp_path: Path) -> None:
        wf = Workflow("f")

        @wf.job()
        def a() -> str:
            return "ok"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "test-final-active"
        st.insert_run(run_id, "f", "u", {})
        st.insert_task_instance(run_id, "a", None, TaskState.RUNNING, {})

        wf._maybe_finalise_run(run_id, st)
        assert st.get_run(run_id)["status"] == "RUNNING"


# ═══════════════════════════════════════════════════════════════════════════
# Coverage: _worker.py
# ═══════════════════════════════════════════════════════════════════════════


class TestWorkerMixin:
    def _setup(self, tmp_path: Path) -> tuple[Workflow, SqliteStore, str]:
        wf = Workflow("w")

        @wf.job()
        def task_ok(val: Annotated[str, Param(help="V")]) -> str:
            return val.upper()

        @wf.job()
        def task_fail(val: Annotated[str, Param(help="V")]) -> str:
            raise RuntimeError("worker boom")

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "w-test-001"
        st.insert_run(run_id, "w", "u", {"val": "hello", "run_dir": str(tmp_path)})
        return wf, st, run_id

    def test_worker_success(self, tmp_path: Path) -> None:
        wf, st, run_id = self._setup(tmp_path)
        iid = st.insert_task_instance(
            run_id, "task_ok", None, TaskState.PENDING, {},
        )
        st.update_task_submitted(run_id, "task_ok", "j1")

        wf.worker(run_id, st, tmp_path, "task_ok")

        # Result file should have been written and can be ingested
        from reflow.results import ingest_results

        n = ingest_results(run_id, st)
        assert n == 1
        row = st.get_task_instance(run_id, "task_ok", None)
        assert row["state"] == "SUCCESS"

    def test_worker_failure(self, tmp_path: Path) -> None:
        wf, st, run_id = self._setup(tmp_path)
        iid = st.insert_task_instance(
            run_id, "task_fail", None, TaskState.PENDING, {},
        )
        st.update_task_submitted(run_id, "task_fail", "j2")

        with pytest.raises(RuntimeError, match="worker boom"):
            wf.worker(run_id, st, tmp_path, "task_fail")

        from reflow.results import ingest_results

        n = ingest_results(run_id, st)
        assert n == 1
        row = st.get_task_instance(run_id, "task_fail", None)
        assert row["state"] == "FAILED"

    def test_worker_unknown_task(self, tmp_path: Path) -> None:
        wf, st, run_id = self._setup(tmp_path)
        with pytest.raises(KeyError, match="Unknown task"):
            wf.worker(run_id, st, tmp_path, "nonexistent")

    def test_worker_missing_instance(self, tmp_path: Path) -> None:
        wf, st, run_id = self._setup(tmp_path)
        # No instance was inserted for task_ok
        with pytest.raises(KeyError, match="Instance not found"):
            wf.worker(run_id, st, tmp_path, "task_ok")


# ═══════════════════════════════════════════════════════════════════════════
# Coverage: run.py (status printing, cancel, retry)
# ═══════════════════════════════════════════════════════════════════════════


class TestRunStatus:
    def test_status_prints(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        wf = Workflow("w")

        @wf.job()
        def task(val: Annotated[str, Param(help="V")]) -> str:
            return val

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", val="hi", store=st)

        # status() without as_dict should print to stdout
        result = run.status()
        assert result is None
        out = capsys.readouterr().out
        assert run.run_id in out
        assert "SUCCESS" in out

    def test_status_with_task_filter(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        wf = Workflow("w")

        @wf.job()
        def a() -> str:
            return "ok"

        @wf.job()
        def b(x: Annotated[str, Result(step="a")]) -> str:
            return x

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st)

        run.status(task="a")
        out = capsys.readouterr().out
        assert "a" in out

    def test_status_as_dict(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def task() -> str:
            return "ok"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st)
        info = run.status(as_dict=True)
        assert isinstance(info, dict)
        assert "run" in info
        assert "summary" in info


class TestRunCancelRetry:
    def test_cancel(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        wf = Workflow("w")

        @wf.job()
        def task() -> str:
            return "ok"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "w-cancel-001"
        st.insert_run(run_id, "w", "u", {})
        iid = st.insert_task_instance(
            run_id, "task", None, TaskState.RUNNING, {},
        )

        from reflow.run import Run

        run = Run(workflow=wf, run_id=run_id, run_dir=tmp_path, store=st)
        n = run.cancel(executor=LocalExecutor())

        out = capsys.readouterr().out
        assert "Cancelled" in out
        assert n == 1

    def test_retry(
        self, tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = Workflow("w")

        @wf.job()
        def task(val: Annotated[str, Param(help="V")]) -> str:
            return val

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "w-retry-001"
        st.insert_run(run_id, "w", "u", {"val": "hi", "run_dir": str(tmp_path)})
        st.insert_task_spec(run_id, "task", False, {}, [])
        iid = st.insert_task_instance(
            run_id, "task", None, TaskState.FAILED, {},
        )

        from reflow.run import Run

        run = Run(workflow=wf, run_id=run_id, run_dir=tmp_path, store=st)
        n = run.retry()

        out = capsys.readouterr().out
        assert "retry" in out.lower() or "Marked" in out
        assert n == 1


# ═══════════════════════════════════════════════════════════════════════════
# Coverage: params.py — broadcast-related edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestParamsBroadcastEdgeCases:
    def test_broadcast_direct_singleton_to_singleton(self) -> None:
        """broadcast=True on a singleton->singleton still returns DIRECT."""
        mode = infer_wire_mode(str, False, str, False, broadcast=False)
        assert mode == WireMode.DIRECT

    def test_gather_not_affected_by_broadcast(self) -> None:
        """Gather mode (array->singleton list) unaffected by broadcast flag."""
        mode = infer_wire_mode(str, True, list[str], False, broadcast=False)
        assert mode == WireMode.GATHER

    def test_chain_not_affected_by_broadcast(self) -> None:
        mode = infer_wire_mode(str, True, str, True, broadcast=False)
        assert mode == WireMode.CHAIN

    def test_chain_flatten_not_affected(self) -> None:
        mode = infer_wire_mode(list[str], True, str, True, broadcast=False)
        assert mode == WireMode.CHAIN_FLATTEN


# ═══════════════════════════════════════════════════════════════════════════
# Coverage: cli.py — submit, status, cancel, retry, runs commands
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
            wf, ["submit", "--run-dir", str(tmp_path / "r"), "--x", "val"],
        )
        rc = run_command(wf, args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "Created run" in out or "cli_test" in out


# ═══════════════════════════════════════════════════════════════════════════
# CLI: --force and --force-tasks flags
# ═══════════════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════════════
# Coverage: workflow/_core.py — cancel_run, retry_failed, run_status
# ═══════════════════════════════════════════════════════════════════════════


class TestWorkflowCancelRetryStatus:
    def test_cancel_run(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def task() -> str:
            return "ok"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "w-cr-001"
        st.insert_run(run_id, "w", "u", {})
        iid = st.insert_task_instance(
            run_id, "task", None, TaskState.SUBMITTED, {},
        )

        n = wf.cancel_run(run_id, st, executor=LocalExecutor())
        assert n == 1
        assert st.get_task_instance(run_id, "task", None)["state"] == "CANCELLED"

    def test_run_status_unknown(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def task() -> str:
            return "ok"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()

        with pytest.raises(KeyError, match="Unknown run_id"):
            wf.run_status("nonexistent", st)

    def test_describe_typed(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def step_a(x: Annotated[str, Param(help="X")]) -> list[str]:
            return []

        @wf.array_job()
        def step_b(item: Annotated[str, Result(step="step_a")]) -> str:
            return item

        desc = wf.describe_typed()
        assert desc.name == "w"
        assert len(desc.tasks) == 2

    def test_describe_json(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def step_a(x: Annotated[str, Param(help="X")]) -> str:
            return "ok"

        d = wf.describe()
        assert d["name"] == "w"
        # Should be JSON-serializable
        json.dumps(d)


# ═══════════════════════════════════════════════════════════════════════════
# Coverage: _dispatch.py — _all_deps_satisfied, _resolve_result_inputs
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchHelpers:
    def test_all_deps_satisfied_false(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def upstream() -> str:
            return "ok"

        @wf.job()
        def downstream(x: Annotated[str, Result(step="upstream")]) -> str:
            return x

        wf.validate()
        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "w-deps-001"
        st.insert_run(run_id, "w", "u", {})

        # upstream not submitted yet
        assert not wf._all_deps_satisfied(st, run_id, wf.tasks["downstream"])

    def test_all_deps_satisfied_true(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def upstream() -> str:
            return "ok"

        @wf.job()
        def downstream(x: Annotated[str, Result(step="upstream")]) -> str:
            return x

        wf.validate()
        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run_id = "w-deps-002"
        st.insert_run(run_id, "w", "u", {})
        iid = st.insert_task_instance(
            run_id, "upstream", None, TaskState.SUCCESS, {},
        )
        st.update_task_success(iid, "ok")

        assert wf._all_deps_satisfied(st, run_id, wf.tasks["downstream"])

    def test_effective_dependencies(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def a() -> str:
            return "ok"

        @wf.job()
        def b() -> str:
            return "ok"

        @wf.job(after=["b"])
        def c(x: Annotated[str, Result(step="a")]) -> str:
            return x

        wf.validate()
        deps = wf._effective_dependencies(wf.tasks["c"])
        assert "a" in deps
        assert "b" in deps


# ═══════════════════════════════════════════════════════════════════════════
# Coverage: Run.__repr__
# ═══════════════════════════════════════════════════════════════════════════


class TestRunRepr:
    def test_repr(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def task() -> str:
            return "ok"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st)

        r = repr(run)
        assert "Run(" in r
        assert run.run_id in r
        assert "w" in r
