"""Tests for reflow."""

from __future__ import annotations

import inspect
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

import pytest

from reflow import (
    Flow, Param, Result, Run, RunDir, RunState, TaskState, Workflow,
)
from reflow.flow import JobConfig, TaskSpec
from reflow.params import (
    WireMode, argparse_type_callable, check_type_compatibility,
    collect_cli_params, collect_result_deps, extract_base_type,
    extract_param, extract_result, get_list_element_type,
    get_literal_values, get_return_type, infer_wire_mode,
    is_list_type, is_run_dir, merge_resolved_params, unwrap_optional,
)
from reflow.manifest import DEFAULT_CODEC, ManifestCodec
from reflow.stores.sqlite import SqliteStore
from reflow.workflow import _build_kwargs


# --- params ----------------------------------------------------------------


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
        assert infer_wire_mode(list[str], True, list[str], False) == WireMode.GATHER_FLATTEN
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


# --- manifest ---------------------------------------------------------------


@dataclass(frozen=True)
class ExamplePayload:
    path: Path
    created_at: datetime


class TestManifestCodec:
    def test_roundtrip_supported_types(self) -> None:
        codec = ManifestCodec()
        value = {
            "path": Path("/tmp/data.nc"),
            "created_at": datetime(2026, 3, 17, 12, 30, tzinfo=timezone.utc),
            "run": UUID("12345678-1234-5678-1234-567812345678"),
            "state": TaskState.SUCCESS,
            "items": ("a", 1),
            "payload": ExamplePayload(
                path=Path("/tmp/out.zarr"),
                created_at=datetime(2026, 3, 17, 12, 45, tzinfo=timezone.utc),
            ),
        }
        restored = codec.loads(codec.dumps(value))
        assert restored["path"] == Path("/tmp/data.nc")
        assert restored["created_at"] == datetime(2026, 3, 17, 12, 30, tzinfo=timezone.utc)
        assert restored["run"] == UUID("12345678-1234-5678-1234-567812345678")
        assert restored["state"] is TaskState.SUCCESS
        assert restored["items"] == ("a", 1)
        assert restored["payload"] == ExamplePayload(
            path=Path("/tmp/out.zarr"),
            created_at=datetime(2026, 3, 17, 12, 45, tzinfo=timezone.utc),
        )

    def test_rejects_non_string_dict_keys(self) -> None:
        with pytest.raises(TypeError, match="string keys"):
            DEFAULT_CODEC.dumps({1: "bad"})


class TestSqliteStore:
    def test_roundtrip(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "test", "user1", {"x": 1})
        run = store.get_run("r1")
        assert run is not None
        assert run["graph_name"] == "test"
        assert run["user_id"] == "user1"

    def test_run_record_methods(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        params = {
            "path": tmp_path / "data.nc",
            "created_at": datetime(2026, 3, 17, 12, 30, tzinfo=timezone.utc),
        }
        store.insert_run("r1", "test", "user1", params)

        record = store.get_run_record("r1")
        assert record is not None
        assert record.run_id == "r1"
        assert record.graph_name == "test"
        assert record.status is RunState.RUNNING
        assert record.parameters["path"] == tmp_path / "data.nc"

        listed = store.list_run_records(user_id="user1")
        assert len(listed) == 1
        assert listed[0] == record

    def test_roundtrip_typed_parameters(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        params = {
            "path": tmp_path / "data.nc",
            "created_at": datetime(2026, 3, 17, 12, 30, tzinfo=timezone.utc),
            "state": TaskState.PENDING,
        }
        store.insert_run("r1", "test", "user1", params)
        restored = store.get_run_parameters("r1")
        assert restored["path"] == tmp_path / "data.nc"
        assert restored["created_at"] == datetime(2026, 3, 17, 12, 30, tzinfo=timezone.utc)
        assert restored["state"] is TaskState.PENDING

    def test_task_instance_record_methods(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "t", None, TaskState.PENDING, {"a": 1})

        record = store.get_task_instance_record("r1", "t", None)
        assert record is not None
        assert record.id == iid
        assert record.state is TaskState.PENDING
        assert record.input == {"a": 1}

        store.update_task_running(iid)
        store.update_task_success(iid, {"out": tmp_path / "result.txt"})

        updated = store.get_task_instance_record("r1", "t", None)
        assert updated is not None
        assert updated.state is TaskState.SUCCESS
        assert updated.output == {"out": tmp_path / "result.txt"}

        listed = store.list_task_instance_records("r1", task_name="t")
        assert len(listed) == 1
        assert listed[0] == updated

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

    def test_describe_typed(self) -> None:
        wf = Workflow("w")

        @wf.job()
        def a(x: Annotated[str, Param(help="X")], run_dir: RunDir) -> str:
            return "ok"

        description = wf.describe_typed()
        assert description.name == "w"
        assert description.tasks[0].name == "a"
        assert description.cli_params[0].name == "x"


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

    def test_submit_uses_shared_store(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        os.environ["REFLOW_MODE"] = "dry-run"
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache-home"))
        wf = self._make_wf()
        run = wf.submit(run_dir=tmp_path / "r", start="2025-01-01", bucket="b")
        assert run.store.path == (tmp_path / "cache-home" / "reflow" / "manifest.db").resolve()

    def test_submit_missing_required(self, tmp_path: Path) -> None:
        wf = self._make_wf()
        with pytest.raises(TypeError, match="missing required"):
            wf.submit(run_dir=tmp_path / "r", start="2025-01-01")

    def test_submit_local(self, tmp_path: Path) -> None:
        os.environ["REFLOW_MODE"] = "dry-run"
        wf = self._make_wf()
        run = wf.submit(
            run_dir=tmp_path / "r", start="2025-01-01",
            bucket="b", executor="local",
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
        args = build_parser(self._make_wf()).parse_args([
            "submit", "--run-dir", "/tmp", "--start", "2025-01-01",
            "--bucket", "b", "--model", "icon",
        ])
        assert args.start == "2025-01-01"
        assert args.model == "icon"

    def test_literal_choices_enforced(self) -> None:
        from reflow.cli import build_parser
        with pytest.raises(SystemExit):
            build_parser(self._make_wf()).parse_args([
                "submit", "--run-dir", "/tmp", "--start", "x",
                "--bucket", "b", "--model", "invalid",
            ])

    def test_hidden_commands(self) -> None:
        from reflow.cli import build_parser, parse_args
        wf = self._make_wf()
        assert "dispatch" not in build_parser(wf).format_help()
        args = parse_args(wf, ["dispatch", "--run-id", "x"])
        assert args._command == "dispatch"

    def test_status_without_run_dir(self) -> None:
        from reflow.cli import build_parser
        args = build_parser(self._make_wf()).parse_args(["status", "run-123"])
        assert args.run_dir is None

    def test_describe_command(self) -> None:
        from reflow.cli import parse_args
        wf = self._make_wf()
        args = parse_args(wf, ["describe"])
        assert args._command == "describe"


# --- _build_kwargs ---------------------------------------------------------


class TestBuildKwargs:
    @staticmethod
    def _spec(func):
        return TaskSpec(
            name="t", func=func, config=JobConfig(),
            signature=inspect.signature(func),
            result_deps=collect_result_deps(func),
            return_type=get_return_type(func),
        )

    def test_run_dir_injection(self) -> None:
        def fn(wd: RunDir) -> None:
            pass
        kw = _build_kwargs(self._spec(fn), {"run_dir": "/scratch"}, {})
        assert kw["wd"] == Path("/scratch")

    def test_result_from_input(self) -> None:
        def fn(item: Annotated[str, Result(step="p")]) -> str:
            return item
        kw = _build_kwargs(self._spec(fn), {}, {"item": "a.nc"})
        assert kw["item"] == "a.nc"

    def test_global_param(self) -> None:
        def fn(bucket: str) -> None:
            pass
        kw = _build_kwargs(self._spec(fn), {"bucket": "b"}, {})
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

    def test_default_store_path_uses_xdg_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from reflow.config import load_config
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache-home"))
        cfg = load_config(Path("/nonexistent"))
        assert cfg.default_store_path == str((tmp_path / "cache-home" / "reflow" / "manifest.db"))


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
        assert verify_cached_output([str(f1), "/nonexistent"], list[Path], None) is False

    def test_non_path_trusts_cache(self) -> None:
        assert verify_cached_output(42, int, None) is True
        assert verify_cached_output("done", str, None) is True

    def test_custom_callable(self) -> None:
        assert verify_cached_output(42, int, lambda x: x > 0) is True
        assert verify_cached_output(-1, int, lambda x: x > 0) is False


class TestCacheStoreIntegration:
    def test_find_cached_record(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})

        ih = compute_input_hash("t", "1", {"x": "a"})
        identity = compute_identity(ih, [])
        iid = store.insert_task_instance(
            "r1", "t", None, TaskState.PENDING, {},
            identity=identity, input_hash=ih,
        )
        store.update_task_running(iid)
        store.update_task_success(iid, {"path": tmp_path / "result.txt"}, output_hash="oh1")

        cached = store.find_cached_record("t", identity)
        assert cached is not None
        assert cached.state is TaskState.SUCCESS
        assert cached.output == {"path": tmp_path / "result.txt"}
        assert cached.output_hash == "oh1"

    def test_find_cached(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})

        ih = compute_input_hash("t", "1", {"x": "a"})
        identity = compute_identity(ih, [])
        iid = store.insert_task_instance(
            "r1", "t", None, TaskState.PENDING, {},
            identity=identity, input_hash=ih,
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
            "r1", "t", None, TaskState.PENDING, {},
            identity=identity, input_hash=ih,
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
            "r1", "t", None, TaskState.PENDING, {},
            identity=id_v1, input_hash=ih_v1,
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
            "r1", "downstream", None, TaskState.PENDING, {},
            identity=id_old, input_hash=ih,
        )
        store.update_task_running(iid)
        store.update_task_success(iid, "old_result", output_hash="oh_old")

        # Same inputs but different upstream hash: cache miss
        id_new = compute_identity(ih, ["upstream_hash_new"])
        assert store.find_cached("downstream", id_new) is None


# --- signals ---------------------------------------------------------------


class TestSignals:
    def test_task_interrupted(self) -> None:
        from reflow.signals import TaskInterrupted
        import signal as sig
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

        cfg = Config({
            "executor": {"account": "default_account"},
            "notifications": {"mail_user": "user@dkrz.de", "mail_type": "FAIL"},
        })
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

        cfg = Config({
            "executor": {"account": "default_account"},
            "notifications": {"mail_user": "default@dkrz.de"},
        })
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
        assert reflow.__version__.startswith("1.")

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
        from reflow.executors.slurm import SlurmExecutor
        from reflow.executors import JobResources

        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test", mail_user="user@dkrz.de",
            mail_type="FAIL,END", signal="B:INT@60",
        )
        cmd = exc._build_sbatch(res, ["python", "test.py"])
        assert "--mail-user" in cmd
        assert "user@dkrz.de" in cmd
        assert "--mail-type" in cmd
        assert "FAIL,END" in cmd
        assert "--signal" in cmd
        assert "B:INT@60" in cmd

    def test_no_mail_when_none(self) -> None:
        from reflow.executors.slurm import SlurmExecutor
        from reflow.executors import JobResources

        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(job_name="test")
        cmd = exc._build_sbatch(res, ["python", "test.py"])
        assert "--mail-user" not in cmd
        assert "--mail-type" not in cmd
        assert "--signal" not in cmd
