"""Tests for new features: runs filtering, bulk cancel, failed instance display."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any
from unittest.mock import patch

import pytest

from reflow import (
    LocalExecutor,
    Param,
    Result,
    RunDir,
    RunState,
    TaskState,
    Workflow,
)
from reflow.stores.sqlite import SqliteStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_store(tmp_path: Path) -> SqliteStore:
    store = SqliteStore(tmp_path / "manifest.db")
    store.init()
    return store


def _make_simple_wf(name: str = "test") -> Workflow:
    wf = Workflow(name)

    @wf.job()
    def step_a(
        x: Annotated[str, Param(help="input")],
        run_dir: RunDir = RunDir(),
    ) -> str:
        return x.upper()

    return wf


def _make_array_wf() -> Workflow:
    wf = Workflow("arraywf")

    @wf.job()
    def produce(
        x: Annotated[str, Param(help="input")],
        run_dir: RunDir = RunDir(),
    ) -> list[str]:
        return [f"{x}_{i}" for i in range(5)]

    @wf.job(array=True)
    def consume(
        item: Annotated[str, Result(step="produce")],
        run_dir: RunDir = RunDir(),
    ) -> str:
        return item.upper()

    return wf


# ═══════════════════════════════════════════════════════════════════════════
# Store: list_runs filtering
# ═══════════════════════════════════════════════════════════════════════════


class TestListRunsFiltering:
    def test_limit(self, tmp_path: Path) -> None:
        store = _make_store(tmp_path)
        for i in range(10):
            store.insert_run(f"run-{i:02d}", "wf", "", {})
        rows = store.list_runs(limit=3)
        assert len(rows) == 3

    def test_limit_none_returns_all(self, tmp_path: Path) -> None:
        store = _make_store(tmp_path)
        for i in range(5):
            store.insert_run(f"run-{i}", "wf", "", {})
        rows = store.list_runs(limit=None)
        assert len(rows) == 5

    def test_status_filter(self, tmp_path: Path) -> None:
        store = _make_store(tmp_path)
        store.insert_run("r1", "wf", "", {})
        store.insert_run("r2", "wf", "", {})
        store.update_run_status("r1", RunState.FAILED)
        rows = store.list_runs(status="FAILED")
        assert len(rows) == 1
        assert rows[0]["run_id"] == "r1"

    def test_since_filter(self, tmp_path: Path) -> None:
        store = _make_store(tmp_path)
        store.insert_run("old", "wf", "", {})
        # All runs are created "now", so filtering with a past date returns all
        rows = store.list_runs(since="2000-01-01T00:00:00")
        assert len(rows) >= 1

    def test_until_filter(self, tmp_path: Path) -> None:
        store = _make_store(tmp_path)
        store.insert_run("r1", "wf", "", {})
        # Until a date far in the past should return nothing
        rows = store.list_runs(until="2000-01-01T00:00:00")
        assert len(rows) == 0

    def test_combined_filters(self, tmp_path: Path) -> None:
        store = _make_store(tmp_path)
        store.insert_run("r1", "wf", "", {})
        store.insert_run("r2", "wf", "", {})
        store.update_run_status("r1", RunState.CANCELLED)
        rows = store.list_runs(status="RUNNING", limit=10)
        assert all(r["status"] == "RUNNING" for r in rows)

    def test_graph_name_plus_limit(self, tmp_path: Path) -> None:
        store = _make_store(tmp_path)
        for i in range(5):
            store.insert_run(f"a-{i}", "alpha", "", {})
            store.insert_run(f"b-{i}", "beta", "", {})
        rows = store.list_runs(graph_name="alpha", limit=2)
        assert len(rows) == 2
        assert all(r["graph_name"] == "alpha" for r in rows)


# ═══════════════════════════════════════════════════════════════════════════
# Workflow: cancel_runs (bulk)
# ═══════════════════════════════════════════════════════════════════════════


class TestCancelRuns:
    def test_cancel_multiple_runs(self, tmp_path: Path) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        store.insert_run("r2", "test", "", {})
        store.insert_task_instance("r1", "step_a", None, TaskState.SUBMITTED, {})
        store.insert_task_instance("r2", "step_a", None, TaskState.SUBMITTED, {})

        with patch.object(LocalExecutor, "cancel"):
            n = wf.cancel_runs(["r1", "r2"], store)
        assert n == 2
        r1 = store.get_run("r1")
        r2 = store.get_run("r2")
        assert r1["status"] == "CANCELLED"
        assert r2["status"] == "CANCELLED"

    def test_cancel_runs_empty_list(self, tmp_path: Path) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        n = wf.cancel_runs([], store)
        assert n == 0

    def test_cancel_runs_with_task_filter(self, tmp_path: Path) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        store.insert_task_instance("r1", "step_a", None, TaskState.SUBMITTED, {})

        with patch.object(LocalExecutor, "cancel"):
            n = wf.cancel_runs(["r1"], store, task_name="step_a")
        assert n == 1
        # Run status should NOT be set to CANCELLED when task_name is given
        r1 = store.get_run("r1")
        assert r1["status"] == "RUNNING"


# ═══════════════════════════════════════════════════════════════════════════
# Workflow: run_status returns failed_instances
# ═══════════════════════════════════════════════════════════════════════════


class TestRunStatusFailedInstances:
    def test_failed_instances_key_present(self, tmp_path: Path) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        store.insert_task_instance("r1", "step_a", None, TaskState.SUCCESS, {})
        info = wf.run_status("r1", store)
        assert "failed_instances" in info
        assert info["failed_instances"] == []

    def test_failed_instances_populated(self, tmp_path: Path) -> None:
        wf = _make_array_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "arraywf", "", {})
        # 3 succeed, 2 fail
        for i in range(3):
            iid = store.insert_task_instance(
                "r1", "consume", i, TaskState.SUBMITTED, {}
            )
            store.update_task_success(iid, f"out_{i}")
        for i in range(3, 5):
            iid = store.insert_task_instance(
                "r1", "consume", i, TaskState.SUBMITTED, {}
            )
            store.update_task_failed(iid, f"Error in element {i}")

        info = wf.run_status("r1", store)
        failed = info["failed_instances"]
        assert len(failed) == 2
        assert all(f["state"] == "FAILED" for f in failed)
        indices = {f["array_index"] for f in failed}
        assert indices == {3, 4}
        assert "Error in element 3" in failed[0]["error_text"]

    def test_failed_instances_include_error_text(self, tmp_path: Path) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        iid = store.insert_task_instance(
            "r1", "step_a", None, TaskState.SUBMITTED, {}
        )
        traceback = "Traceback (most recent call last):\n  File ...\nValueError: bad"
        store.update_task_failed(iid, traceback)
        info = wf.run_status("r1", store)
        assert len(info["failed_instances"]) == 1
        assert "ValueError: bad" in info["failed_instances"][0]["error_text"]


# ═══════════════════════════════════════════════════════════════════════════
# CLI: runs command with filters
# ═══════════════════════════════════════════════════════════════════════════


class TestCLIRunsFiltering:
    def _setup(self, tmp_path: Path) -> tuple[Workflow, SqliteStore]:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        for i in range(25):
            store.insert_run(f"run-{i:03d}", "test", "", {})
        return wf, store

    def test_runs_default_last_20(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf, store = self._setup(tmp_path)
        from reflow.cli import build_parser, run_command, parse_args

        args = parse_args(wf, ["runs", "--store-path", str(store.path)])
        with patch(
            "reflow.cli._make_store", return_value=store
        ):
            run_command(wf, args)
        out = capsys.readouterr().out
        # Should show 20 runs + the hint line
        run_lines = [l for l in out.splitlines() if l.strip().startswith("run-")]
        assert len(run_lines) == 20
        assert "--all" in out

    def test_runs_all_flag(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf, store = self._setup(tmp_path)
        from reflow.cli import parse_args, run_command

        args = parse_args(wf, ["runs", "--all", "--store-path", str(store.path)])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        run_lines = [l for l in out.splitlines() if l.strip().startswith("run-")]
        assert len(run_lines) == 25

    def test_runs_last_n(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf, store = self._setup(tmp_path)
        from reflow.cli import parse_args, run_command

        args = parse_args(wf, ["runs", "--last", "5", "--store-path", str(store.path)])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        run_lines = [l for l in out.splitlines() if l.strip().startswith("run-")]
        assert len(run_lines) == 5

    def test_runs_status_filter(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r-ok", "test", "", {})
        store.insert_run("r-fail", "test", "", {})
        store.update_run_status("r-fail", RunState.FAILED)

        from reflow.cli import parse_args, run_command

        args = parse_args(wf, [
            "runs", "--status", "FAILED", "--store-path", str(store.path)
        ])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        assert "r-fail" in out
        assert "r-ok" not in out


# ═══════════════════════════════════════════════════════════════════════════
# CLI: cancel with multiple IDs
# ═══════════════════════════════════════════════════════════════════════════


class TestCLICancelMultiple:
    def test_cancel_multiple_ids_with_yes(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        store.insert_run("r2", "test", "", {})
        store.insert_task_instance("r1", "step_a", None, TaskState.SUBMITTED, {})
        store.insert_task_instance("r2", "step_a", None, TaskState.SUBMITTED, {})

        from reflow.cli import parse_args, run_command

        args = parse_args(wf, [
            "cancel", "r1", "r2", "--yes", "--store-path", str(store.path)
        ])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        assert "2" in out  # "Cancelled 2 task instance(s)"

    def test_cancel_single_id_no_prompt(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        store.insert_task_instance("r1", "step_a", None, TaskState.SUBMITTED, {})

        from reflow.cli import parse_args, run_command

        # Single ID should not prompt even without --yes
        args = parse_args(wf, [
            "cancel", "r1", "--store-path", str(store.path)
        ])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        assert "Cancelled" in out

    def test_cancel_multi_aborted_without_yes(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        store.insert_run("r2", "test", "", {})

        from reflow.cli import parse_args, run_command

        args = parse_args(wf, [
            "cancel", "r1", "r2", "--store-path", str(store.path)
        ])
        with patch("reflow.cli._make_store", return_value=store), \
             patch("builtins.input", return_value="n"):
            rc = run_command(wf, args)
        assert rc == 1
        out = capsys.readouterr().out
        assert "Aborted" in out


# ═══════════════════════════════════════════════════════════════════════════
# CLI: status --errors
# ═══════════════════════════════════════════════════════════════════════════


class TestCLIStatusErrors:
    def _setup_failed_run(self, tmp_path: Path) -> tuple[Workflow, SqliteStore]:
        wf = _make_array_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "arraywf", "", {})
        # produce succeeds
        pid = store.insert_task_instance(
            "r1", "produce", None, TaskState.SUBMITTED, {}
        )
        store.update_task_success(pid, ["a", "b", "c", "d", "e"])
        # consume: 3 succeed, 2 fail
        for i in range(3):
            cid = store.insert_task_instance(
                "r1", "consume", i, TaskState.SUBMITTED, {}
            )
            store.update_task_success(cid, f"OUT_{i}")
        for i in range(3, 5):
            cid = store.insert_task_instance(
                "r1", "consume", i, TaskState.SUBMITTED, {}
            )
            store.update_task_failed(
                cid,
                f"Traceback (most recent call last):\n"
                f"  File \"worker.py\", line 42\n"
                f"RuntimeError: element {i} failed",
            )
        return wf, store

    def test_status_shows_failed_indices(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf, store = self._setup_failed_run(tmp_path)
        from reflow.cli import parse_args, run_command

        args = parse_args(wf, [
            "status", "r1", "--store-path", str(store.path)
        ])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        assert "[3]" in out
        assert "[4]" in out
        assert "FAILED" in out
        # Without --errors, traceback should NOT appear
        assert "RuntimeError" not in out

    def test_status_errors_shows_traceback(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf, store = self._setup_failed_run(tmp_path)
        from reflow.cli import parse_args, run_command

        args = parse_args(wf, [
            "status", "r1", "--errors", "--store-path", str(store.path)
        ])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        assert "RuntimeError: element 3 failed" in out
        assert "RuntimeError: element 4 failed" in out

    def test_status_long_traceback_truncated(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        iid = store.insert_task_instance(
            "r1", "step_a", None, TaskState.SUBMITTED, {}
        )
        # Create a traceback longer than 15 lines
        long_tb = "\n".join(f"  line {i}: something" for i in range(30))
        store.update_task_failed(iid, long_tb)

        from reflow.cli import parse_args, run_command

        args = parse_args(wf, [
            "status", "r1", "--errors", "--store-path", str(store.path)
        ])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        assert "more lines" in out

    def test_status_json_includes_failed_instances(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf, store = self._setup_failed_run(tmp_path)
        from reflow.cli import parse_args, run_command

        args = parse_args(wf, [
            "status", "r1", "--json", "--store-path", str(store.path)
        ])
        with patch("reflow.cli._make_store", return_value=store):
            run_command(wf, args)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert "failed_instances" in data
        assert len(data["failed_instances"]) == 2


# ═══════════════════════════════════════════════════════════════════════════
# Run handle: status(errors=True)
# ═══════════════════════════════════════════════════════════════════════════


class TestRunHandleStatus:
    def test_run_status_shows_failed(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        iid = store.insert_task_instance(
            "r1", "step_a", None, TaskState.SUBMITTED, {}
        )
        store.update_task_failed(iid, "ValueError: oops")

        from reflow.run import Run

        run = Run(wf, "r1", tmp_path, store)
        run.status()
        out = capsys.readouterr().out
        assert "FAILED" in out

    def test_run_status_errors_flag(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        iid = store.insert_task_instance(
            "r1", "step_a", None, TaskState.SUBMITTED, {}
        )
        store.update_task_failed(iid, "ValueError: oops")

        from reflow.run import Run

        run = Run(wf, "r1", tmp_path, store)
        run.status(errors=True)
        out = capsys.readouterr().out
        assert "ValueError: oops" in out

    def test_run_status_as_dict_has_failed_key(self, tmp_path: Path) -> None:
        wf = _make_simple_wf()
        store = _make_store(tmp_path)
        store.insert_run("r1", "test", "", {})
        store.insert_task_instance("r1", "step_a", None, TaskState.SUCCESS, {})

        from reflow.run import Run

        run = Run(wf, "r1", tmp_path, store)
        info = run.status(as_dict=True)
        assert "failed_instances" in info
        assert info["failed_instances"] == []
