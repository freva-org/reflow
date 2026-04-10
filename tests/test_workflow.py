"""test_workflow.py - refactored reflow tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Annotated, Literal
from unittest.mock import patch

import pytest

from reflow import (
    Config,
    Flow,
    LocalExecutor,
    Param,
    Result,
    Run,
    RunDir,
    TaskState,
    Workflow,
)
from reflow.params import (
    extract_base_type,
    extract_param,
    extract_result,
    get_list_element_type,
    get_literal_values,
    is_list_type,
    is_run_dir,
    unwrap_optional,
)
from reflow.stores.sqlite import SqliteStore
from reflow.workflow._helpers import default_executor, make_run_id, resolve_executor, resolve_index
from reflow.results import ingest_results

# ═══════════════════════════════════════════════════════════════════════════
# Coverage: _dispatch.py  (dispatch loop, resolve, fan-out, finalize)
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════


class TestWorkflow:
    def test_include(self) -> None:
        f = Flow("f")

        @f.job()
        def a() -> str:
            return "ok"

        wf = Workflow("w")
        wf.include(f)
        assert "a" in wf.tasks

    def test_include_collision(self) -> None:
        f = Flow("f")

        @f.job()
        def a() -> str:
            return "ok"

        wf = Workflow("w")
        wf.include(f)
        with pytest.raises(ValueError, match="already exists"):
            wf.include(f)

    def test_validate_ok(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def prepare(run_dir: RunDir) -> list[str]:
            return []

        @wf.array_job()
        def convert(item: Annotated[str, Result(step="prepare")]) -> str:
            return item

        wf.validate()

    def test_validate_type_error(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def prepare() -> str:
            return "x"

        @wf.array_job()
        def convert(item: Annotated[str, Result(step="prepare")]) -> str:
            return item

        with pytest.raises(TypeError, match="fan-out"):
            wf.validate()

    def test_cycle(self) -> None:
        wf = Workflow("w")

        @wf.job(after=["b"])
        def a() -> None:
            pass

        @wf.job(after=["a"])
        def b() -> None:
            pass

        with pytest.raises(ValueError, match="cycle"):
            wf.validate()

    def test_describe(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def a(x: Annotated[str, Param(help="X")], run_dir: RunDir) -> str:
            return "ok"

        manifest = wf.describe()
        assert manifest["name"] == "w"
        assert len(manifest["tasks"]) == 1
        assert manifest["cli_params"][0]["name"] == "x"


class TestSubmitAndRun:
    @staticmethod
    def _make_wf() -> Workflow:
        wf = Workflow("submit_test")

        @wf.job()
        def prepare(
            start: Annotated[str, Param(help="Start")],
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

    def test_submit_returns_run(self, tmp_path: Path) -> None:
        os.environ["REFLOW_MODE"] = "dry-run"
        wf = self._make_wf()
        run = wf.submit(run_dir=tmp_path / "r", start="2025-01-01", bucket="b")
        assert isinstance(run, Run)
        assert run.run_id
        assert "submit_test" in run.run_id

    def test_submit_missing_required(self, tmp_path: Path) -> None:
        wf = self._make_wf()
        with pytest.raises(TypeError, match="missing required"):
            wf.submit(run_dir=tmp_path / "r", start="2025-01-01")

    def test_submit_local(self, tmp_path: Path) -> None:
        os.environ["REFLOW_MODE"] = "dry-run"
        wf = self._make_wf()
        run = wf.submit(
            run_dir=tmp_path / "r",
            start="2025-01-01",
            bucket="b",
            executor="local",
        )
        assert isinstance(run, Run)

    def test_run_status(self, tmp_path: Path) -> None:
        os.environ["REFLOW_MODE"] = "dry-run"
        wf = self._make_wf()
        run = wf.submit(run_dir=tmp_path / "r", start="2025-01-01", bucket="b")
        info = run.status(as_dict=True)
        assert "run" in info and "summary" in info

    def test_run_repr(self) -> None:
        wf = Workflow("test")
        store = SqliteStore(Path("/tmp/fake.db"))
        run = Run(workflow=wf, run_id="test-123", run_dir=Path("/tmp"), store=store)
        assert "test-123" in repr(run)


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


class TestDescriptors:
    def test_result_single(self) -> None:
        r = Result(step="foo")
        assert r.steps == ["foo"]

    def test_result_multi(self) -> None:
        r = Result(steps=["a", "b"])
        assert r.steps == ["a", "b"]

    def test_result_invalid(self) -> None:
        with pytest.raises(ValueError):
            Result(step="a", steps=["b"])
        with pytest.raises(ValueError):
            Result()

    def test_extract_result(self) -> None:
        assert extract_result(Annotated[str, Result(step="x")]) is not None
        assert extract_result(str) is None

    def test_extract_param(self) -> None:
        assert extract_param(Annotated[str, Param(help="h")]) is not None
        assert extract_param(int) is None

    def test_is_run_dir(self) -> None:
        assert is_run_dir(RunDir)
        assert not is_run_dir(str)

    def test_extract_base_type(self) -> None:
        assert extract_base_type(Annotated[int, Param(help="x")]) is int
        assert extract_base_type(str) is str

    def test_unwrap_optional(self) -> None:
        inner, opt = unwrap_optional(str | None)
        assert inner is str and opt

    def test_list_helpers(self) -> None:
        assert get_list_element_type(list[str]) is str
        assert is_list_type(list[int])
        assert not is_list_type(str)

    def test_literal_values(self) -> None:
        vals = get_literal_values(Literal["a", "b", "c"])
        assert vals == ("a", "b", "c")
        assert get_literal_values(str) is None


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


class TestWorkflowForce:
    def test_submit_with_force(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = Workflow("fw")

        @wf.job()
        def prepare(
            x: Annotated[str, Param(help="X")],
            run_dir: RunDir = RunDir(),
        ) -> list[str]:
            return []

        run = wf.submit(
            run_dir=tmp_path / "r",
            x="val",
            force=True,
        )
        params = run.store.get_run_parameters(run.run_id)
        assert params.get("__force__") is True

    def test_submit_with_force_tasks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        wf = Workflow("fw")

        @wf.job()
        def prepare(
            x: Annotated[str, Param(help="X")],
            run_dir: RunDir = RunDir(),
        ) -> list[str]:
            return []

        run = wf.submit(
            run_dir=tmp_path / "r",
            x="val",
            force_tasks=["prepare"],
        )
        params = run.store.get_run_parameters(run.run_id)
        assert "prepare" in params.get("__force_tasks__", [])


class TestWorkflowValidateExtended:
    def test_unknown_after_dependency(self) -> None:
        wf = Workflow("w")

        @wf.job(after=["nonexistent"])
        def a() -> None:
            pass

        with pytest.raises(ValueError, match="unknown task"):
            wf.validate()

    def test_unknown_result_step(self) -> None:
        wf = Workflow("w")

        @wf.array_job()
        def a(item: Annotated[str, Result(step="nonexistent")]) -> str:
            return item

        with pytest.raises(ValueError, match="unknown task"):
            wf.validate()

    def test_include_with_prefix(self) -> None:
        f = Flow("f")

        @f.job()
        def prep() -> list[str]:
            return []

        @f.array_job()
        def conv(item: Annotated[str, Result(step="prep")]) -> str:
            return item

        wf = Workflow("w")
        wf.include(f, prefix="era5")
        assert "era5_prep" in wf.tasks
        assert "era5_conv" in wf.tasks
        wf.validate()


class TestWorkflowHelpers:
    def test_make_run_id_format(self) -> None:
        rid = make_run_id("test")
        parts = rid.split("-")
        assert parts[0] == "test"
        assert len(parts) == 3
        assert len(parts[2]) == 4  # short hex

    def test_resolve_executor_local(self) -> None:
        exc = resolve_executor("local")
        assert isinstance(exc, LocalExecutor)

    def test_resolve_executor_unknown(self) -> None:
        with pytest.raises(ValueError, match="Unknown executor"):
            resolve_executor("unicorn")

    def test_resolve_executor_none(self) -> None:
        assert resolve_executor(None) is None

    def test_resolve_executor_passthrough(self) -> None:
        exc = LocalExecutor()
        assert resolve_executor(exc) is exc

    def test_resolve_index_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "5")
        assert resolve_index(None) == 5

    def test_resolve_index_explicit(self) -> None:
        assert resolve_index(3) == 3

    def test_resolve_index_no_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SLURM_ARRAY_TASK_ID", raising=False)
        assert resolve_index(None) is None
