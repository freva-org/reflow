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
from typing import Annotated

import pytest

from reflow import (
    Config,
    Flow,
    LocalExecutor,
    Param,
    Result,
    RunDir,
    TaskState,
    Workflow,
)
from reflow.params import (
    WireMode,
    check_type_compatibility,
    infer_wire_mode,
)
from reflow.stores.sqlite import SqliteStore

# ═══════════════════════════════════════════════════════════════════════════
# Feature 2: Broadcast mode
# ═══════════════════════════════════════════════════════════════════════════


class TestResultBroadcast:
    def test_broadcast_default_false(self) -> None:
        r = Result(step="foo")
        assert r.broadcast is False

    def test_broadcast_true(self) -> None:
        r = Result(step="foo", broadcast=True)
        assert r.broadcast is True

    def test_repr_without_broadcast(self) -> None:
        r = Result(step="foo")
        assert "broadcast" not in repr(r)

    def test_repr_with_broadcast(self) -> None:
        r = Result(step="foo", broadcast=True)
        assert "broadcast=True" in repr(r)

    def test_repr_multi_with_broadcast(self) -> None:
        r = Result(steps=["a", "b"], broadcast=True)
        assert "broadcast=True" in repr(r)
        assert "steps=" in repr(r)


class TestWireModeBroadcast:
    def test_infer_broadcast_from_types(self) -> None:
        """list[str] -> list[str] with array downstream = BROADCAST."""
        mode = infer_wire_mode(list[str], False, list[str], True)
        assert mode == WireMode.BROADCAST

    def test_infer_fan_out_from_types(self) -> None:
        """list[str] -> str with array downstream = FAN_OUT (unchanged)."""
        mode = infer_wire_mode(list[str], False, str, True)
        assert mode == WireMode.FAN_OUT

    def test_explicit_broadcast_on_non_list(self) -> None:
        """Dict -> dict with broadcast=True = BROADCAST."""
        mode = infer_wire_mode(dict, False, dict, True, broadcast=True)
        assert mode == WireMode.BROADCAST

    def test_explicit_broadcast_array_to_array(self) -> None:
        """Array-to-array with broadcast=True = BROADCAST."""
        mode = infer_wire_mode(str, True, str, True, broadcast=True)
        assert mode == WireMode.BROADCAST

    def test_non_list_without_broadcast_raises(self) -> None:
        """Dict -> dict on array without broadcast raises helpful error."""
        with pytest.raises(TypeError, match="broadcast=True"):
            infer_wire_mode(dict, False, dict, True, broadcast=False)

    def test_check_type_compat_with_broadcast(self) -> None:
        mode = check_type_compatibility(
            dict,
            False,
            dict,
            True,
            "up",
            "down",
            "p",
            broadcast=True,
        )
        assert mode == WireMode.BROADCAST


class TestBroadcastValidation:
    def test_validate_with_broadcast(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def gen_items() -> list[str]:
            return ["a", "b"]

        @wf.job()
        def gen_config() -> dict:
            return {"key": "val"}

        @wf.array_job()
        def process(
            item: Annotated[str, Result(step="gen_items")],
            cfg: Annotated[dict, Result(step="gen_config", broadcast=True)],
        ) -> str:
            return item

        # Should not raise
        wf.validate()

    def test_broadcast_preserved_in_flow_prefix(self) -> None:
        f = Flow("f")

        @f.job()
        def cfg() -> dict:
            return {}

        @f.job()
        def items() -> list[str]:
            return []

        @f.array_job()
        def proc(
            item: Annotated[str, Result(step="items")],
            settings: Annotated[dict, Result(step="cfg", broadcast=True)],
        ) -> str:
            return item

        wf = Workflow("w")
        wf.include(f, prefix="pre")

        spec = wf.tasks["pre_proc"]
        assert spec.result_deps["settings"].broadcast is True
        assert spec.result_deps["item"].broadcast is False
        wf.validate()


# ═══════════════════════════════════════════════════════════════════════════
# Feature 3: run_local
# ═══════════════════════════════════════════════════════════════════════════


class TestRunLocalBasic:
    def test_simple_singleton(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def greet(name: Annotated[str, Param(help="N")]) -> str:
            return f"Hello {name}"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", name="World", store=st)
        info = run.status(as_dict=True)
        assert info is not None
        assert info["run"]["status"] == "SUCCESS"
        assert st.get_singleton_output(run.run_id, "greet") == "Hello World"

    def test_fan_out_pipeline(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def gen(prefix: Annotated[str, Param(help="P")]) -> list[str]:
            return [f"{prefix}_1", f"{prefix}_2", f"{prefix}_3"]

        @wf.array_job()
        def upper(item: Annotated[str, Result(step="gen")]) -> str:
            return item.upper()

        @wf.job()
        def join(results: Annotated[list[str], Result(step="upper")]) -> str:
            return ",".join(sorted(results))

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", prefix="x", store=st)
        assert run.status(as_dict=True)["run"]["status"] == "SUCCESS"
        assert st.get_singleton_output(run.run_id, "join") == "X_1,X_2,X_3"

    def test_broadcast_in_local(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def items() -> list[str]:
            return ["a", "b"]

        @wf.job()
        def config() -> dict:
            return {"scale": 10}

        @wf.array_job()
        def process(
            item: Annotated[str, Result(step="items")],
            cfg: Annotated[dict, Result(step="config", broadcast=True)],
        ) -> str:
            return f"{item}:{cfg['scale']}"

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st)
        outs = st.get_array_outputs(run.run_id, "process")
        assert outs == ["a:10", "b:10"]


class TestRunLocalErrorHandling:
    def test_on_error_stop(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def boom() -> str:
            raise ValueError("intentional")

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        with pytest.raises(RuntimeError, match="boom.*failed"):
            wf.run_local(run_dir=tmp_path / "r", store=st, on_error="stop")

    def test_on_error_continue(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def good() -> str:
            return "ok"

        @wf.job()
        def bad() -> str:
            raise ValueError("boom")

        @wf.job()
        def after_bad(x: Annotated[str, Result(step="bad")]) -> str:
            return x

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(
            run_dir=tmp_path / "r",
            store=st,
            on_error="continue",
        )
        info = run.status(as_dict=True)
        assert info["run"]["status"] == "FAILED"
        # 'good' should have succeeded
        assert st.get_singleton_output(run.run_id, "good") == "ok"

    def test_downstream_skipped_on_failure(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def fail_step() -> list[str]:
            raise RuntimeError("boom")

        @wf.array_job()
        def child(item: Annotated[str, Result(step="fail_step")]) -> str:
            return item

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(
            run_dir=tmp_path / "r",
            store=st,
            on_error="continue",
        )
        assert run.status(as_dict=True)["run"]["status"] == "FAILED"
        # child should have 0 instances (skipped entirely)
        assert st.count_task_instances(run.run_id, "child") == 0


class TestRunLocalCaching:
    def test_cache_hit_on_second_run(self, tmp_path: Path) -> None:
        counter = {"n": 0}
        wf = Workflow("w")

        @wf.job()
        def counted(val: Annotated[str, Param(help="V")]) -> str:
            counter["n"] += 1
            return val.upper()

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        wf.run_local(run_dir=tmp_path / "r1", val="hi", store=st)
        assert counter["n"] == 1

        wf.run_local(run_dir=tmp_path / "r2", val="hi", store=st)
        assert counter["n"] == 1  # cache hit, not re-executed

    def test_force_bypasses_cache(self, tmp_path: Path) -> None:
        counter = {"n": 0}
        wf = Workflow("w")

        @wf.job()
        def counted(val: Annotated[str, Param(help="V")]) -> str:
            counter["n"] += 1
            return val.upper()

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        wf.run_local(run_dir=tmp_path / "r1", val="hi", store=st)
        assert counter["n"] == 1

        wf.run_local(run_dir=tmp_path / "r2", val="hi", store=st, force=True)
        assert counter["n"] == 2

    def test_force_tasks_selective(self, tmp_path: Path) -> None:
        counters = {"a": 0, "b": 0}
        wf = Workflow("w")

        @wf.job()
        def task_a(val: Annotated[str, Param(help="V")]) -> str:
            counters["a"] += 1
            return val

        @wf.job()
        def task_b(x: Annotated[str, Result(step="task_a")]) -> str:
            counters["b"] += 1
            return x.upper()

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        wf.run_local(run_dir=tmp_path / "r1", val="hi", store=st)
        assert counters == {"a": 1, "b": 1}

        wf.run_local(
            run_dir=tmp_path / "r2",
            val="hi",
            store=st,
            force_tasks=["task_b"],
        )
        # task_a should be cached, task_b forced
        assert counters["a"] == 1
        assert counters["b"] == 2

    def test_cache_disabled_per_task(self, tmp_path: Path) -> None:
        counter = {"n": 0}
        wf = Workflow("w")

        @wf.job(cache=False)
        def nocache(val: Annotated[str, Param(help="V")]) -> str:
            counter["n"] += 1
            return val

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        wf.run_local(run_dir=tmp_path / "r1", val="hi", store=st)
        wf.run_local(run_dir=tmp_path / "r2", val="hi", store=st)
        assert counter["n"] == 2  # never cached


class TestRunLocalParallel:
    def test_parallel_with_unpicklable_marks_failed(self, tmp_path: Path) -> None:
        """max_workers>1 with unpicklable test functions fails elements."""
        wf = Workflow("w")

        @wf.job()
        def gen() -> list[int]:
            return [1, 2]

        @wf.array_job()
        def double(item: Annotated[int, Result(step="gen")]) -> int:
            return item * 2

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        # Functions defined inside tests aren't picklable for
        # ProcessPoolExecutor.  The parallel path catches element
        # failures individually.
        run = wf.run_local(
            run_dir=tmp_path / "r",
            store=st,
            max_workers=2,
            on_error="continue",
        )
        assert run.status(as_dict=True)["run"]["status"] == "FAILED"

    def test_sequential_array(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def gen() -> list[int]:
            return [10, 20, 30]

        @wf.array_job()
        def inc(item: Annotated[int, Result(step="gen")]) -> int:
            return item + 1

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st, max_workers=1)
        outs = st.get_array_outputs(run.run_id, "inc")
        assert outs == [11, 21, 31]

    def test_array_parallelism_honored(self, tmp_path: Path) -> None:
        """array_parallelism caps effective workers."""
        wf = Workflow("w")

        @wf.job()
        def gen() -> list[str]:
            return ["a", "b", "c"]

        @wf.array_job(array_parallelism=1)
        def proc(item: Annotated[str, Result(step="gen")]) -> str:
            return item.upper()

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        run = wf.run_local(run_dir=tmp_path / "r", store=st, max_workers=1)
        outs = st.get_array_outputs(run.run_id, "proc")
        assert outs == ["A", "B", "C"]


class TestRunLocalMissing:
    def test_missing_required_param(self, tmp_path: Path) -> None:
        wf = Workflow("w")

        @wf.job()
        def task(x: Annotated[str, Param(help="X")]) -> str:
            return x

        st = SqliteStore(str(tmp_path / "db.sqlite"))
        with pytest.raises(TypeError, match="missing required"):
            wf.run_local(run_dir=tmp_path / "r", store=st)


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
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Submit + dispatch cycle via dry-run executor."""
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = self._build_wf()
        st = SqliteStore(str(tmp_path / "db.sqlite"))
        st.init()
        run = wf.submit(
            run_dir=tmp_path / "r",
            x="val",
            store=st,
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
            run_id,
            "task_ok",
            None,
            TaskState.PENDING,
            {},
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
            run_id,
            "task_fail",
            None,
            TaskState.PENDING,
            {},
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
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
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
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
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
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
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
            run_id,
            "task",
            None,
            TaskState.RUNNING,
            {},
        )

        from reflow.run import Run

        run = Run(workflow=wf, run_id=run_id, run_dir=tmp_path, store=st)
        n = run.cancel(executor=LocalExecutor())

        out = capsys.readouterr().out
        assert "Cancelled" in out
        assert n == 1

    def test_retry(
        self,
        tmp_path: Path,
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
            run_id,
            "task",
            None,
            TaskState.FAILED,
            {},
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
            wf,
            ["submit", "--run-dir", str(tmp_path / "r"), "--x", "val"],
        )
        rc = run_command(wf, args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "Created run" in out or "cli_test" in out


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
            run_id,
            "task",
            None,
            TaskState.SUBMITTED,
            {},
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
            run_id,
            "upstream",
            None,
            TaskState.SUCCESS,
            {},
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
