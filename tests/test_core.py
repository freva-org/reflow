"""Tests for reflow."""

from __future__ import annotations

import inspect
import os
from pathlib import Path
from typing import Annotated, Literal

import pytest

from reflow import (
    Config,
    Flow,
    JobResources,
    Param,
    Result,
    Run,
    RunDir,
    SlurmExecutor,
    TaskState,
    Workflow,
    ensure_config_exists,
)
from reflow.flow import JobConfig, TaskSpec
from reflow.params import (
    WireMode,
    collect_cli_params,
    collect_result_deps,
    extract_base_type,
    extract_param,
    extract_result,
    get_list_element_type,
    get_literal_values,
    get_return_type,
    infer_wire_mode,
    is_list_type,
    is_run_dir,
    unwrap_optional,
)
from reflow.stores.sqlite import SqliteStore
from reflow.workflow import build_kwargs
from reflow.workflow._core import _suggest_name


# --- params ----------------------------------------------------------------
class TestSuggestName:
    def test_close_match(self) -> None:
        hint = _suggest_name("preprae", {"prepare", "compute", "finalize"})
        assert "prepare" in hint

    def test_no_match(self) -> None:
        hint = _suggest_name("zzzzzzz", {"prepare", "compute"})
        assert hint == ""

    def test_exact_match_not_suggested(self) -> None:
        # If user typo is very close to two names, still returns one
        hint = _suggest_name("comput", {"compute", "compile"})
        assert hint != ""

    def test_works_with_dict(self) -> None:
        hint = _suggest_name("preprae", {"prepare": 1, "compute": 2})
        assert "prepare" in hint


class TestDidYouMeanValidation:
    def test_result_step_typo(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def prepare() -> list[str]:
            return []

        @wf.array_job()
        def compute(item: Annotated[str, Result(step="preprae")]) -> str:
            return item

        with pytest.raises(ValueError, match="Did you mean 'prepare'"):
            wf.validate()

    def test_after_typo(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def step_a() -> str:
            return "ok"

        @wf.job(after=["step_aa"])
        def step_b() -> str:
            return "ok"

        with pytest.raises(ValueError, match="Did you mean 'step_a'"):
            wf.validate()

    def test_no_suggestion_for_unrelated(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def alpha() -> list[str]:
            return []

        @wf.array_job()
        def beta(v: Annotated[str, Result(step="zzzzzzzzz")]) -> str:
            return v

        with pytest.raises(ValueError, match="unknown task") as exc_info:
            wf.validate()
        assert "Did you mean" not in str(exc_info.value)

    def test_topological_order_typo_in_after(self) -> None:
        """_topological_order also gives suggestions."""
        wf = Workflow("w")

        @wf.job()
        def first() -> str:
            return "ok"

        @wf.job(after=["firs"])
        def second() -> str:
            return "ok"

        with pytest.raises(ValueError, match="Did you mean"):
            wf.validate()


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


class TestWireMode:
    def test_all_modes(self) -> None:
        assert infer_wire_mode(list[str], False, str, True) == WireMode.FAN_OUT
        assert infer_wire_mode(str, True, list[str], False) == WireMode.GATHER
        assert infer_wire_mode(str, True, str, True) == WireMode.CHAIN
        assert infer_wire_mode(list[str], True, str, True) == WireMode.CHAIN_FLATTEN
        assert (
            infer_wire_mode(list[str], True, list[str], False)
            == WireMode.GATHER_FLATTEN
        )
        assert infer_wire_mode(str, False, str, False) == WireMode.DIRECT

    def test_fan_out_error(self) -> None:
        with pytest.raises(TypeError, match="fan-out"):
            infer_wire_mode(str, False, str, True)

    def test_gather_error(self) -> None:
        with pytest.raises(TypeError, match="gather"):
            infer_wire_mode(str, True, str, False)


class TestCollectCliParams:
    def test_skips_result_and_rundir(self) -> None:
        def fn(
            item: Annotated[str, Result(step="p")],
            wd: RunDir,
            bucket: Annotated[str, Param(help="B")],
        ) -> None:
            pass

        params = collect_cli_params("t", fn, inspect.signature(fn))
        names = [p.name for p in params]
        assert "bucket" in names
        assert "item" not in names
        assert "wd" not in names

    def test_literal_choices(self) -> None:
        def fn(
            model: Annotated[Literal["era5", "icon"], Param(help="M")] = "era5",
        ) -> None:
            pass

        params = collect_cli_params("t", fn, inspect.signature(fn))
        assert params[0].literal_choices == ("era5", "icon")

    def test_local_namespace(self) -> None:
        def fn(
            chunk: Annotated[int, Param(namespace="local")] = 256,
        ) -> None:
            pass

        params = collect_cli_params("convert", fn, inspect.signature(fn))
        assert params[0].cli_flag() == "--convert-chunk"


# --- SqliteStore -----------------------------------------------------------


class TestSqliteStore:
    def test_roundtrip(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "test", "user1", {"x": 1})
        run = store.get_run("r1")
        assert run is not None
        assert run["graph_name"] == "test"
        assert run["user_id"] == "user1"

    def test_task_lifecycle(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "t", None, TaskState.PENDING, {"a": 1})
        row = store.get_task_instance("r1", "t", None)
        assert row is not None and row["state"] == "PENDING"

        store.update_task_running(iid)
        assert store.get_task_instance("r1", "t", None)["state"] == "RUNNING"

        store.update_task_success(iid, ["out.nc"])
        assert store.get_task_instance("r1", "t", None)["state"] == "SUCCESS"

    def test_singleton_output(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "prep", None, TaskState.PENDING, {})
        store.update_task_running(iid)
        store.update_task_success(iid, ["a.nc", "b.nc"])
        assert store.get_singleton_output("r1", "prep") == ["a.nc", "b.nc"]

    def test_array_outputs(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        for idx, val in enumerate(["x.zarr", "y.zarr"]):
            iid = store.insert_task_instance("r1", "conv", idx, TaskState.PENDING, {})
            store.update_task_running(iid)
            store.update_task_success(iid, val)
        assert store.get_array_outputs("r1", "conv") == ["x.zarr", "y.zarr"]

    def test_dependency_check(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "t", None, TaskState.PENDING, {})
        assert not store.dependency_is_satisfied("r1", "t")
        store.update_task_running(iid)
        store.update_task_success(iid, None)
        assert store.dependency_is_satisfied("r1", "t")

    def test_state_summary(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        store.insert_task_instance("r1", "a", None, TaskState.SUCCESS, {})
        store.insert_task_instance("r1", "b", 0, TaskState.SUCCESS, {})
        store.insert_task_instance("r1", "b", 1, TaskState.FAILED, {})
        summary = store.task_state_summary("r1")
        assert summary["a"] == {"SUCCESS": 1}
        assert summary["b"] == {"SUCCESS": 1, "FAILED": 1}

    def test_user_filter(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "alice", {})
        store.insert_run("r2", "g", "bob", {})
        assert len(store.list_runs(user_id="alice")) == 1

    def test_cancel_and_retry(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "t", None, TaskState.RUNNING, {})
        store.update_task_cancelled(iid)
        assert store.get_task_instance("r1", "t", None)["state"] == "CANCELLED"
        store.mark_for_retry(iid)
        assert store.get_task_instance("r1", "t", None)["state"] == "RETRYING"


# --- Flow ------------------------------------------------------------------


class TestFlow:
    def test_registration(self) -> None:
        f = Flow("test")

        @f.job()
        def a(run_dir: RunDir) -> list[str]:
            return []

        @f.array_job()
        def b(item: Annotated[str, Result(step="a")]) -> str:
            return item

        assert set(f.tasks) == {"a", "b"}

    def test_prefix(self) -> None:
        f = Flow("test")

        @f.job()
        def prepare(run_dir: RunDir) -> list[str]:
            return []

        @f.array_job()
        def convert(item: Annotated[str, Result(step="prepare")]) -> str:
            return item

        tasks, order = f._prefixed_tasks("era5")
        assert "era5_prepare" in tasks
        conv = tasks["era5_convert"]
        for dep in conv.result_deps.values():
            assert dep.steps == ["era5_prepare"]


# --- Workflow --------------------------------------------------------------


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


# --- submit and Run --------------------------------------------------------


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


# --- CLI -------------------------------------------------------------------


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


# --- build_kwargs ---------------------------------------------------------


class TestBuildKwargs:
    @staticmethod
    def _spec(func):
        return TaskSpec(
            name="t",
            func=func,
            config=JobConfig(),
            signature=inspect.signature(func),
            result_deps=collect_result_deps(func),
            return_type=get_return_type(func),
        )

    def test_run_dir_injection(self) -> None:
        def fn(wd: RunDir) -> None:
            pass

        kw = build_kwargs(self._spec(fn), {"run_dir": "/scratch"}, {})
        assert kw["wd"] == Path("/scratch")

    def test_result_from_input(self) -> None:
        def fn(item: Annotated[str, Result(step="p")]) -> str:
            return item

        kw = build_kwargs(self._spec(fn), {}, {"item": "a.nc"})
        assert kw["item"] == "a.nc"

    def test_global_param(self) -> None:
        def fn(bucket: str) -> None:
            pass

        kw = build_kwargs(self._spec(fn), {"bucket": "b"}, {})
        assert kw["bucket"] == "b"


# --- config ----------------------------------------------------------------


class TestConfig:
    def test_load_missing(self, tmp_path: Path) -> None:
        from reflow.config import load_config

        cfg = load_config(tmp_path / "nonexistent.toml")
        assert cfg.executor_partition is None

    def test_env_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from reflow.config import load_config

        monkeypatch.setenv("REFLOW_PARTITION", "gpu")
        cfg = load_config(Path("/nonexistent"))
        assert cfg.executor_partition == "gpu"


# --- cache -----------------------------------------------------------------


from reflow.cache import (
    compute_identity,
    compute_input_hash,
    compute_output_hash,
    verify_cached_output,
)


class TestCacheHashing:
    def test_input_hash_deterministic(self) -> None:
        h1 = compute_input_hash("t", "1", {"x": "a"})
        h2 = compute_input_hash("t", "1", {"x": "a"})
        assert h1 == h2

    def test_input_hash_version_matters(self) -> None:
        h1 = compute_input_hash("t", "1", {"x": "a"})
        h2 = compute_input_hash("t", "2", {"x": "a"})
        assert h1 != h2

    def test_input_hash_inputs_matter(self) -> None:
        h1 = compute_input_hash("t", "1", {"x": "a"})
        h2 = compute_input_hash("t", "1", {"x": "b"})
        assert h1 != h2

    def test_identity_includes_upstream(self) -> None:
        ih = compute_input_hash("t", "1", {})
        id1 = compute_identity(ih, ["upstream_hash_a"])
        id2 = compute_identity(ih, ["upstream_hash_b"])
        assert id1 != id2

    def test_identity_deterministic(self) -> None:
        ih = compute_input_hash("t", "1", {})
        id1 = compute_identity(ih, ["a", "b"])
        id2 = compute_identity(ih, ["a", "b"])
        assert id1 == id2

    def test_output_hash(self) -> None:
        h1 = compute_output_hash(["a.nc", "b.nc"])
        h2 = compute_output_hash(["a.nc", "b.nc"])
        h3 = compute_output_hash(["a.nc", "c.nc"])
        assert h1 == h2
        assert h1 != h3


class TestCacheVerify:
    def test_path_exists(self, tmp_path: Path) -> None:
        f = tmp_path / "test.nc"
        f.touch()
        assert verify_cached_output(str(f), Path, None) is True

    def test_path_missing(self) -> None:
        assert verify_cached_output("/nonexistent_abc123", Path, None) is False

    def test_list_path(self, tmp_path: Path) -> None:
        f1 = tmp_path / "a.nc"
        f2 = tmp_path / "b.nc"
        f1.touch()
        f2.touch()
        assert verify_cached_output([str(f1), str(f2)], list[Path], None) is True

    def test_list_path_one_missing(self, tmp_path: Path) -> None:
        f1 = tmp_path / "a.nc"
        f1.touch()
        assert (
            verify_cached_output([str(f1), "/nonexistent"], list[Path], None) is False
        )

    def test_non_path_trusts_cache(self) -> None:
        assert verify_cached_output(42, int, None) is True
        assert verify_cached_output("done", str, None) is True

    def test_custom_callable(self) -> None:
        assert verify_cached_output(42, int, lambda x: x > 0) is True
        assert verify_cached_output(-1, int, lambda x: x > 0) is False


class TestCacheStoreIntegration:
    def test_find_cached(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})

        ih = compute_input_hash("t", "1", {"x": "a"})
        identity = compute_identity(ih, [])
        iid = store.insert_task_instance(
            "r1",
            "t",
            None,
            TaskState.PENDING,
            {},
            identity=identity,
            input_hash=ih,
        )
        store.update_task_running(iid)
        oh = compute_output_hash("result")
        store.update_task_success(iid, "result", output_hash=oh)

        cached = store.find_cached("t", identity)
        assert cached is not None
        assert cached["output"] == "result"
        assert cached["output_hash"] == oh

    def test_find_cached_miss(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        assert store.find_cached("t", "nonexistent") is None

    def test_cross_run_cache(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})

        ih = compute_input_hash("t", "1", {"x": "a"})
        identity = compute_identity(ih, [])
        iid = store.insert_task_instance(
            "r1",
            "t",
            None,
            TaskState.PENDING,
            {},
            identity=identity,
            input_hash=ih,
        )
        store.update_task_running(iid)
        store.update_task_success(iid, "result", output_hash="oh1")

        # New run, same identity should still find it
        store.insert_run("r2", "g", "u", {})
        cached = store.find_cached("t", identity)
        assert cached is not None

    def test_version_bump_invalidates(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})

        ih_v1 = compute_input_hash("t", "1", {"x": "a"})
        id_v1 = compute_identity(ih_v1, [])
        iid = store.insert_task_instance(
            "r1",
            "t",
            None,
            TaskState.PENDING,
            {},
            identity=id_v1,
            input_hash=ih_v1,
        )
        store.update_task_running(iid)
        store.update_task_success(iid, "result_v1", output_hash="oh1")

        # Version 2: different identity, cache miss
        ih_v2 = compute_input_hash("t", "2", {"x": "a"})
        id_v2 = compute_identity(ih_v2, [])
        assert store.find_cached("t", id_v2) is None

    def test_upstream_change_propagates(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})

        ih = compute_input_hash("downstream", "1", {"x": "a"})
        id_old = compute_identity(ih, ["upstream_hash_old"])
        iid = store.insert_task_instance(
            "r1",
            "downstream",
            None,
            TaskState.PENDING,
            {},
            identity=id_old,
            input_hash=ih,
        )
        store.update_task_running(iid)
        store.update_task_success(iid, "old_result", output_hash="oh_old")

        # Same inputs but different upstream hash: cache miss
        id_new = compute_identity(ih, ["upstream_hash_new"])
        assert store.find_cached("downstream", id_new) is None


# --- signals ---------------------------------------------------------------


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


# --- config wiring ---------------------------------------------------------


class TestConfigWiring:
    def test_workflow_loads_config(self) -> None:
        wf = Workflow("test")
        assert wf.config is not None

    def test_config_fallback_in_resources(self, tmp_path: Path) -> None:
        """Config values fill in where decorator doesn't specify."""
        from reflow.config import Config

        cfg = Config(
            {
                "executor": {"submit_options": {"account": "default_account"}},
                "notifications": {"mail_user": "user@dkrz.de", "mail_type": "FAIL"},
            }
        )
        wf = Workflow("test", config=cfg)

        @wf.job()
        def task_a(run_dir: RunDir = RunDir()) -> str:
            return "ok"

        spec = wf.tasks["task_a"]
        res = wf._single_resources(tmp_path, spec)
        assert res.account == "default_account"
        assert res.mail_user == "user@dkrz.de"
        assert res.mail_type == "FAIL"

    def test_decorator_overrides_config(self, tmp_path: Path) -> None:
        """Decorator values take priority over config."""
        from reflow.config import Config

        cfg = Config(
            {
                "executor": {"submit_options": {"account": "default_account"}},
                "notifications": {"mail_user": "default@dkrz.de"},
            }
        )
        wf = Workflow("test", config=cfg)

        @wf.job(account="override_account", mail_user="override@dkrz.de")
        def task_a(run_dir: RunDir = RunDir()) -> str:
            return "ok"

        spec = wf.tasks["task_a"]
        res = wf._single_resources(tmp_path, spec)
        assert res.account == "override_account"
        assert res.mail_user == "override@dkrz.de"


# --- version ---------------------------------------------------------------


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


# --- mail/signal in sbatch -------------------------------------------------


class TestSlurmMailSignal:
    def test_mail_flags_in_sbatch(self) -> None:
        from reflow.executors import JobResources
        from reflow.executors.slurm import SlurmExecutor

        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={
                "mail_user": "user@dkrz.de",
                "mail_type": "FAIL,END",
                "signal": "B:INT@60",
            },
        )
        cmd = exc._build_sbatch(res, ["python", "test.py"])
        assert "--mail-user" in cmd
        assert "user@dkrz.de" in cmd
        assert "--mail-type" in cmd
        assert "FAIL,END" in cmd
        assert "--signal" in cmd
        assert "B:INT@60" in cmd

    def test_no_mail_when_none(self) -> None:
        from reflow.executors import JobResources
        from reflow.executors.slurm import SlurmExecutor

        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(job_name="test")
        cmd = exc._build_sbatch(res, ["python", "test.py"])
        assert "--mail-user" not in cmd
        assert "--mail-type" not in cmd
        assert "--signal" not in cmd


class TestSchedulerNativeOptions:
    def test_flow_collects_submit_options(self) -> None:
        flow = Flow("sched")

        @flow.job(partition="compute", qos="debug", queue="workq")
        def task() -> str:
            return "ok"

        cfg = flow.tasks["task"].config
        assert cfg.submit_options["partition"] == "compute"
        assert cfg.submit_options["qos"] == "debug"
        assert cfg.submit_options["queue"] == "workq"

    def test_workflow_applies_config_defaults(self, tmp_path: Path) -> None:
        wf = Workflow(
            "sched",
            config=Config(
                {
                    "executor": {
                        "submit_options": {"partition": "cfgpart", "account": "cfgacc"}
                    }
                }
            ),
        )

        @wf.job()
        def task() -> str:
            return "ok"

        resources = wf._single_resources(tmp_path, wf.tasks["task"])
        assert resources.submit_options["partition"] == "cfgpart"
        assert resources.submit_options["account"] == "cfgacc"

    def test_code_submit_options_override_config(self, tmp_path: Path) -> None:
        wf = Workflow(
            "sched",
            config=Config(
                {
                    "executor": {
                        "submit_options": {"partition": "cfgpart", "account": "cfgacc"}
                    }
                }
            ),
        )

        @wf.job(partition="codepart", qos="debug")
        def task() -> str:
            return "ok"

        resources = wf._single_resources(tmp_path, wf.tasks["task"])
        assert resources.submit_options["partition"] == "codepart"
        assert resources.submit_options["qos"] == "debug"
        assert resources.submit_options["account"] == "cfgacc"

    def test_env_overrides_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REFLOW_PARTITION", "envpart")
        monkeypatch.setenv("REFLOW_ACCOUNT", "envacc")
        wf = Workflow(
            "sched",
            config=Config({"executor": {"partition": "cfgpart", "account": "cfgacc"}}),
        )

        @wf.job()
        def task() -> str:
            return "ok"

        resources = wf._single_resources(tmp_path, wf.tasks["task"])
        assert resources.submit_options["partition"] == "envpart"
        assert resources.submit_options["account"] == "envacc"

    def test_slurm_executor_renders_submit_options(self) -> None:
        executor = SlurmExecutor(mode="dry-run")
        resources = JobResources(
            job_name="job",
            submit_options={
                "partition": "compute",
                "qos": "debug",
                "exclusive": True,
                "exclude": ["n1", "n2"],
            },
        )
        cmd = executor._build_sbatch(resources, ["python", "worker.py"])
        assert "--partition" in cmd
        assert "compute" in cmd
        assert "--qos" in cmd
        assert "debug" in cmd
        assert "--exclusive" in cmd
        assert cmd.count("--exclude") == 2


class TestConfigHelpers:
    def test_ensure_config_exists(self, tmp_path: Path) -> None:
        path = ensure_config_exists(tmp_path / "config.toml")
        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert "[executor]" in content
        assert "# partition =" in content
