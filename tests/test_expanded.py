"""Expanded tests to increase reflow coverage.

Targets modules / paths not sufficiently covered by test_core.py and
test_results.py:

- manifest.py (ManifestCodec round-trips, edge cases, typed descriptions)
- config.py (rosetta stone, dispatch options, env overrides, write failures)
- stores/records.py (RunRecord, TaskInstanceRecord, TaskSpecRecord)
- stores/sqlite.py (read-only mode, retry decorator, edge cases)
- executors/local.py (LocalExecutor lifecycle)
- executors/slurm.py (sbatch rendering, dry-run, from_environment)
- flow.py (duplicate registration, prefix edge cases, array_job validation)
- cli.py (dag, describe, runs, hidden worker parser, main entry)
- workflow.py (build_kwargs locals, _maybe_finalise_run, force/force_tasks)
- cache.py (edge cases in verify_cached_output)
- _types.py (TaskState/RunState enum helpers)
- params.py (merge_resolved_params dedup, ResolvedParam methods)
"""

from __future__ import annotations

import inspect
import json
import signal as sig
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

import pytest

from reflow import (
    Config,
    Flow,
    Param,
    Result,
    RunDir,
    RunState,
    TaskState,
    Workflow,
    write_example_config,
)
from reflow._types import TaskState as TS
from reflow.cache import (
    compute_output_hash,
    verify_cached_output,
)
from reflow.config import (
    _rosetta_stone,
    config_path,
)
from reflow.executors import JobResources
from reflow.executors.local import LocalExecutor
from reflow.executors.slurm import SlurmExecutor
from reflow.flow import JobConfig, TaskSpec
from reflow.manifest import (
    DEFAULT_CODEC,
    CliParamDescription,
    TaskDescription,
    WorkflowDescription,
    canonical_manifest_dumps,
    manifest_dumps,
    manifest_loads,
)
from reflow.params import (
    ResolvedParam,
    argparse_type_callable,
    check_type_compatibility,
    collect_result_deps,
    get_return_type,
    merge_resolved_params,
)
from reflow.stores.records import RunRecord, TaskInstanceRecord, TaskSpecRecord
from reflow.stores.sqlite import SqliteStore
from reflow.workflow import build_kwargs, make_run_id, resolve_executor, resolve_index

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════


class TestTaskStateEnumHelpers:
    def test_terminal_states(self) -> None:
        terminal = TS.terminal()
        assert TS.SUCCESS in terminal
        assert TS.FAILED in terminal
        assert TS.CANCELLED in terminal
        assert TS.RUNNING not in terminal

    def test_active_states(self) -> None:
        active = TS.active()
        assert TS.PENDING in active
        assert TS.SUBMITTED in active
        assert TS.RUNNING in active
        assert TS.RETRYING in active
        assert TS.SUCCESS not in active

    def test_cancellable_states(self) -> None:
        cancellable = TS.cancellable()
        assert TS.PENDING in cancellable
        assert TS.SUBMITTED in cancellable
        assert TS.RUNNING in cancellable
        assert TS.FAILED not in cancellable

    def test_retriable_states(self) -> None:
        retriable = TS.retriable()
        assert TS.FAILED in retriable
        assert TS.CANCELLED in retriable
        assert TS.SUCCESS not in retriable

    def test_str_enum_value(self) -> None:
        assert TS.PENDING.value == "PENDING"
        assert str(TS.PENDING) == "TaskState.PENDING"


class TestRunStateEnum:
    def test_values(self) -> None:
        assert RunState.RUNNING.value == "RUNNING"
        assert RunState.SUCCESS.value == "SUCCESS"
        assert RunState.FAILED.value == "FAILED"
        assert RunState.CANCELLED.value == "CANCELLED"


# ═══════════════════════════════════════════════════════════════════════════
# manifest.py — ManifestCodec
# ═══════════════════════════════════════════════════════════════════════════


class TestManifestCodecPrimitives:
    def test_none(self) -> None:
        assert DEFAULT_CODEC.dump_value(None) is None
        assert DEFAULT_CODEC.load_value(None) is None

    def test_bool(self) -> None:
        assert DEFAULT_CODEC.dump_value(True) is True
        assert DEFAULT_CODEC.load_value(False) is False

    def test_int_float_str(self) -> None:
        assert DEFAULT_CODEC.dump_value(42) == 42
        assert DEFAULT_CODEC.dump_value(3.14) == 3.14
        assert DEFAULT_CODEC.dump_value("hello") == "hello"

    def test_list_roundtrip(self) -> None:
        val = [1, "two", 3.0, None]
        dumped = DEFAULT_CODEC.dump_value(val)
        assert DEFAULT_CODEC.load_value(dumped) == val

    def test_dict_roundtrip(self) -> None:
        val = {"a": 1, "b": "two"}
        dumped = DEFAULT_CODEC.dump_value(val)
        assert DEFAULT_CODEC.load_value(dumped) == val


class TestManifestCodecSpecialTypes:
    def test_path_roundtrip(self) -> None:
        p = Path("/scratch/data/test.nc")
        dumped = DEFAULT_CODEC.dump_value(p)
        assert dumped["__reflow_manifest__"] is True
        assert dumped["type"] == "path"
        loaded = DEFAULT_CODEC.load_value(dumped)
        assert loaded == p

    def test_datetime_roundtrip(self) -> None:
        dt = datetime(2025, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
        loaded = DEFAULT_CODEC.load_value(DEFAULT_CODEC.dump_value(dt))
        assert loaded == dt

    def test_uuid_roundtrip(self) -> None:
        u = uuid.uuid4()
        loaded = DEFAULT_CODEC.load_value(DEFAULT_CODEC.dump_value(u))
        assert loaded == u

    def test_tuple_roundtrip(self) -> None:
        t = (1, "two", Path("/a"))
        loaded = DEFAULT_CODEC.load_value(DEFAULT_CODEC.dump_value(t))
        assert loaded == t
        assert isinstance(loaded, tuple)

    def test_enum_roundtrip(self) -> None:
        dumped = DEFAULT_CODEC.dump_value(TaskState.SUCCESS)
        loaded = DEFAULT_CODEC.load_value(dumped)
        assert loaded == TaskState.SUCCESS

    def test_dataclass_roundtrip(self) -> None:
        from reflow.stores.records import RunRecord

        r = RunRecord(
            run_id="r1",
            graph_name="test",
            user_id="u",
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            status=RunState.RUNNING,
            parameters={},
        )
        dumped = DEFAULT_CODEC.dump_value(r)
        assert dumped["type"] == "dataclass"
        loaded = DEFAULT_CODEC.load_value(dumped)
        assert isinstance(loaded, RunRecord)
        assert loaded.run_id == "r1"


class TestManifestCodecErrors:
    def test_non_string_dict_key_raises(self) -> None:
        with pytest.raises(TypeError, match="string keys"):
            DEFAULT_CODEC.dump_value({1: "bad"})

    def test_unsupported_type_raises(self) -> None:
        with pytest.raises(TypeError, match="Unsupported manifest value type"):
            DEFAULT_CODEC.dump_value(set([1, 2]))

    def test_unknown_tagged_type_raises(self) -> None:
        payload = {"__reflow_manifest__": True, "type": "alien", "value": 42}
        with pytest.raises(TypeError, match="Unknown manifest payload type"):
            DEFAULT_CODEC.load_value(payload)

    def test_load_non_dict_non_list_raises(self) -> None:
        with pytest.raises(TypeError, match="Unsupported manifest payload"):
            DEFAULT_CODEC.load_value(object())


class TestManifestCodecDumpsLoads:
    def test_dumps_loads_roundtrip(self) -> None:
        val = {"key": [1, Path("/a"), None]}
        s = DEFAULT_CODEC.dumps(val)
        assert isinstance(s, str)
        loaded = DEFAULT_CODEC.loads(s)
        assert loaded["key"][1] == Path("/a")

    def test_dumps_pretty(self) -> None:
        s = DEFAULT_CODEC.dumps({"a": 1}, pretty=True)
        assert "\n" in s

    def test_canonical_dumps(self) -> None:
        s1 = canonical_manifest_dumps({"b": 2, "a": 1})
        s2 = canonical_manifest_dumps({"a": 1, "b": 2})
        assert s1 == s2  # sorted keys

    def test_loads_bytes(self) -> None:
        val = manifest_loads(b'{"x": 1}')
        assert val == {"x": 1}

    def test_module_level_helpers(self) -> None:
        s = manifest_dumps(42)
        assert manifest_loads(s) == 42


class TestManifestDescriptions:
    def test_cli_param_to_manifest(self) -> None:
        desc = CliParamDescription(
            name="start",
            task="prepare",
            flag="--start",
            type_repr="str",
            required=True,
            default=None,
            namespace="global",
            help="Start date",
            choices=None,
        )
        d = desc.to_manifest_dict()
        assert d["name"] == "start"
        assert d["required"] is True

    def test_task_description_to_manifest(self) -> None:
        desc = TaskDescription(
            name="convert",
            is_array=True,
            config={"cpus": 4},
            dependencies=["prepare"],
            result_deps={"item": {"steps": ["prepare"]}},
            return_type="str",
            has_verify=False,
        )
        d = desc.to_manifest_dict()
        assert d["is_array"] is True
        assert d["dependencies"] == ["prepare"]

    def test_workflow_description_to_manifest(self) -> None:
        desc = WorkflowDescription(
            name="test",
            entrypoint=Path("/bin/test.py"),
            python=Path("/usr/bin/python3"),
            working_dir=Path("/tmp"),
            tasks=[],
            cli_params=[],
        )
        d = desc.to_manifest_dict()
        assert d["name"] == "test"
        assert d["schema_version"] == 1


# ═══════════════════════════════════════════════════════════════════════════
# config.py
# ═══════════════════════════════════════════════════════════════════════════


class TestRosettaStone:
    def test_sbatch_keys(self) -> None:
        assert _rosetta_stone("sbatch", "partition") == "partition"
        assert _rosetta_stone("sbatch", "account") == "account"

    def test_dry_run_falls_back_to_sbatch(self) -> None:
        assert _rosetta_stone("dry-run", "partition") == "partition"

    def test_unknown_key_raises(self) -> None:
        with pytest.raises(KeyError):
            _rosetta_stone("sbatch", "nonexistent_key")


class TestConfigProperties:
    def test_dispatch_properties(self) -> None:
        cfg = Config(
            {
                "dispatch": {
                    "cpus": 2,
                    "time": "01:00:00",
                    "mem": "4G",
                    "partition": "debug",
                    "account": "myacc",
                }
            }
        )
        assert cfg.dispatch_cpus == "2"
        assert cfg.dispatch_time == "01:00:00"
        assert cfg.dispatch_mem == "4G"
        assert cfg.dispatch_partition == "debug"
        assert cfg.dispatch_account == "myacc"

    def test_dispatch_submit_options(self) -> None:
        cfg = Config(
            {
                "dispatch": {"partition": "debug", "account": "acc"},
            }
        )
        opts = cfg.dispatch_submit_options
        assert opts["partition"] == "debug"
        assert opts["account"] == "acc"

    def test_empty_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Clear any env vars that might be set by other tests.
        for var in (
            "REFLOW_MODE",
            "REFLOW_PARTITION",
            "REFLOW_ACCOUNT",
            "REFLOW_PYTHON",
            "REFLOW_MAIL_USER",
        ):
            monkeypatch.delenv(var, raising=False)
        cfg = Config()
        assert cfg.executor_partition is None
        assert cfg.executor_account is None
        assert cfg.mail_user is None
        assert cfg.executor_mode is None
        assert cfg.server_url is None

    def test_default_store_path(self) -> None:
        cfg = Config()
        assert "manifest.db" in cfg.default_store_path

    def test_executor_submit_options_from_config(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for var in (
            "REFLOW_MODE",
            "REFLOW_PARTITION",
            "REFLOW_ACCOUNT",
            "REFLOW_SIGNAL",
            "REFLOW_MAIL_USER",
            "REFLOW_MAIL_TYPE",
        ):
            monkeypatch.delenv(var, raising=False)
        cfg = Config(
            {
                "executor": {"submit_options": {"partition": "gpu", "account": "acc"}},
                "notifications": {"mail_user": "u@e.org", "mail_type": "FAIL"},
            }
        )
        opts = cfg.executor_submit_options
        assert opts["partition"] == "gpu"
        assert opts["account"] == "acc"
        assert opts["mail_user"] == "u@e.org"

    def test_env_override_executor_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        cfg = Config()
        assert cfg.executor_mode == "dry-run"

    def test_env_override_python(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REFLOW_PYTHON", "/custom/python")
        cfg = Config()
        assert cfg.executor_python == "/custom/python"


class TestConfigFileHelpers:
    def test_write_example_config_overwrite_false(self, tmp_path: Path) -> None:
        p = tmp_path / "config.toml"
        write_example_config(p)
        with pytest.raises(FileExistsError):
            write_example_config(p, overwrite=False)

    def test_write_example_config_overwrite_true(self, tmp_path: Path) -> None:
        p = tmp_path / "config.toml"
        write_example_config(p)
        write_example_config(p, overwrite=True)
        assert p.exists()

    def test_config_path_xdg(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("XDG_CONFIG_HOME", "/custom/config")
        assert config_path() == Path("/custom/config/reflow/config.toml")


# ═══════════════════════════════════════════════════════════════════════════
# stores/records.py
# ═══════════════════════════════════════════════════════════════════════════


class TestRunRecord:
    def test_to_public_dict(self) -> None:
        r = RunRecord(
            run_id="r1",
            graph_name="test",
            user_id="alice",
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            status=RunState.RUNNING,
            parameters={"x": 1},
        )
        d = r.to_public_dict()
        assert d["run_id"] == "r1"
        assert d["status"] == "RUNNING"
        assert isinstance(d["created_at"], str)


class TestTaskInstanceRecord:
    def test_to_public_dict(self) -> None:
        r = TaskInstanceRecord(
            id=1,
            run_id="r1",
            task_name="t",
            array_index=None,
            state=TaskState.SUCCESS,
            job_id="12345",
            input={"a": 1},
            output="result",
            error_text=None,
            identity="abc",
            input_hash="def",
            output_hash="ghi",
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            updated_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        d = r.to_public_dict()
        assert d["state"] == "SUCCESS"
        assert d["job_id"] == "12345"
        assert d["output"] == "result"


class TestTaskSpecRecord:
    def test_fields(self) -> None:
        r = TaskSpecRecord(
            run_id="r1",
            task_name="prepare",
            is_array=False,
            config={"cpus": 1},
            dependencies=["upstream"],
        )
        assert r.task_name == "prepare"
        assert not r.is_array


# ═══════════════════════════════════════════════════════════════════════════
# stores/sqlite.py — additional coverage
# ═══════════════════════════════════════════════════════════════════════════


class TestSqliteStoreExtended:
    def test_default_path(self) -> None:
        p = SqliteStore.default_path()
        assert p.name == "manifest.db"

    def test_update_run_status(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        store.update_run_status("r1", RunState.SUCCESS)
        run = store.get_run("r1")
        assert run is not None
        assert run["status"] == "SUCCESS"

    def test_list_runs_by_graph(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "alpha", "u", {})
        store.insert_run("r2", "beta", "u", {})
        alpha_runs = store.list_runs(graph_name="alpha")
        assert len(alpha_runs) == 1
        assert alpha_runs[0]["run_id"] == "r1"

    def test_get_run_parameters_missing(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        with pytest.raises(KeyError):
            store.get_run_parameters("nonexistent")

    def test_get_run_missing(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        assert store.get_run("nonexistent") is None

    def test_insert_task_spec_and_deps(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        store.insert_task_spec("r1", "convert", True, {"cpus": 4}, ["prepare"])
        deps = store.list_task_dependencies("r1", "convert")
        assert deps == ["prepare"]

    def test_count_task_instances_zero(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        assert store.count_task_instances("r1", "nonexistent") == 0

    def test_list_task_instances_by_state(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        store.insert_task_instance("r1", "t", 0, TaskState.SUCCESS, {})
        store.insert_task_instance("r1", "t", 1, TaskState.FAILED, {})

        success_only = store.list_task_instances("r1", states=[TaskState.SUCCESS])
        assert len(success_only) == 1
        assert success_only[0]["state"] == "SUCCESS"

    def test_close_idempotent(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.close()
        store.close()  # should not raise

    def test_get_output_hash_singleton(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "t", None, TaskState.PENDING, {})
        store.update_task_success(iid, "out", output_hash="hash123")
        assert store.get_output_hash("r1", "t") == "hash123"

    def test_get_output_hash_array(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "t", 0, TaskState.PENDING, {})
        store.update_task_success(iid, "out", output_hash="h0")
        assert store.get_output_hash("r1", "t", 0) == "h0"

    def test_get_output_hash_missing(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        assert store.get_output_hash("r1", "t") == ""

    def test_get_all_output_hashes(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        for idx in range(3):
            iid = store.insert_task_instance("r1", "t", idx, TaskState.PENDING, {})
            store.update_task_success(iid, f"out_{idx}", output_hash=f"h{idx}")
        hashes = store.get_all_output_hashes("r1", "t")
        assert hashes == ["h0", "h1", "h2"]

    def test_find_cached_empty_identity(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        assert store.find_cached("t", "") is None

    def test_update_task_failed(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "t", None, TaskState.RUNNING, {})
        store.update_task_failed(iid, "boom")
        row = store.get_task_instance("r1", "t", None)
        assert row is not None
        assert row["state"] == "FAILED"
        assert "boom" in (row.get("error_text") or "")

    def test_typed_record_methods(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "alice", {"key": "val"})
        record = store.get_run_record("r1")
        assert record is not None
        assert record.user_id == "alice"
        assert record.parameters == {"key": "val"}

    def test_task_spec_record_method(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        store.insert_task_spec("r1", "t", False, {"cpus": 1}, [])
        rec = store.get_task_spec_record("r1", "t")
        assert rec is not None
        assert rec.task_name == "t"

    def test_task_instance_record_method(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        iid = store.insert_task_instance("r1", "t", None, TaskState.PENDING, {"a": 1})
        rec = store.get_task_instance_record("r1", "t", None)
        assert rec is not None
        assert rec.id == iid
        assert rec.input == {"a": 1}

    def test_list_task_instance_records(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        store.insert_task_instance("r1", "t", 0, TaskState.PENDING, {})
        store.insert_task_instance("r1", "t", 1, TaskState.SUCCESS, {})
        recs = store.list_task_instance_records("r1", task_name="t")
        assert len(recs) == 2

    def test_list_run_records(self, tmp_path: Path) -> None:
        store = SqliteStore.for_run_dir(tmp_path)
        store.init()
        store.insert_run("r1", "g", "u", {})
        store.insert_run("r2", "g", "u", {})
        recs = store.list_run_records()
        assert len(recs) == 2


# ═══════════════════════════════════════════════════════════════════════════
# executors/local.py
# ═══════════════════════════════════════════════════════════════════════════


class TestLocalExecutor:
    def test_submit_returns_job_id(self) -> None:
        exc = LocalExecutor(capture_output=True)
        jid = exc.submit(JobResources(job_name="test"), ["echo", "hello"])
        assert jid.startswith("local-")

    def test_job_state_completed(self) -> None:
        exc = LocalExecutor(capture_output=True)
        jid = exc.submit(JobResources(job_name="test"), ["true"])
        assert exc.job_state(jid) == "COMPLETED"

    def test_job_state_failed(self) -> None:
        exc = LocalExecutor(capture_output=True)
        jid = exc.submit(JobResources(job_name="test"), ["false"])
        assert exc.job_state(jid) == "FAILED"

    def test_job_state_unknown(self) -> None:
        exc = LocalExecutor()
        assert exc.job_state("unknown-id") is None

    def test_cancel_noop(self) -> None:
        exc = LocalExecutor()
        exc.cancel("some-id")  # should not raise


# ═══════════════════════════════════════════════════════════════════════════
# executors/slurm.py
# ═══════════════════════════════════════════════════════════════════════════


class TestSlurmExecutorExtended:
    def test_dry_run_returns_dryrun_id(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        jid = exc.submit(JobResources(job_name="test"), ["echo"])
        assert jid == "DRYRUN"

    def test_cancel_dryrun_noop(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        exc.cancel("DRYRUN")  # should not raise

    def test_job_state_dryrun(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        assert exc.job_state("DRYRUN") is None

    def test_from_environment_defaults(self) -> None:
        exc = SlurmExecutor.from_environment(Config())
        assert exc.sbatch == "sbatch"
        assert exc.scancel == "scancel"
        assert exc.sacct == "sacct"

    def test_from_environment_env_override(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("REFLOW_MODE", "dry-run")
        monkeypatch.setenv("REFLOW_SBATCH", "/custom/sbatch")
        exc = SlurmExecutor.from_environment()
        assert exc.mode == "dry-run"
        assert exc.sbatch == "/custom/sbatch"

    def test_sbatch_array_rendering(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(job_name="test", array="0-9%4")
        cmd = exc._build_sbatch(res, ["python", "w.py"])
        assert "--array" in cmd
        assert "0-9%4" in cmd

    def test_sbatch_output_error_paths(self, tmp_path: Path) -> None:
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            output_path=tmp_path / "out.log",
            error_path=tmp_path / "err.log",
        )
        cmd = exc._build_sbatch(res, ["echo"])
        assert "--output" in cmd
        assert "--error" in cmd

    def test_render_submit_option_none(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        assert exc._render_submit_option("key", None) == []
        assert exc._render_submit_option("key", False) == []

    def test_render_submit_option_bool_true(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        assert exc._render_submit_option("exclusive", True) == ["--exclusive"]

    def test_render_submit_option_list(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        result = exc._render_submit_option("exclude", ["n1", "n2"])
        assert result == ["--exclude", "n1", "--exclude", "n2"]

    def test_skips_internal_keys(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"python": "/usr/bin/python", "partition": "gpu"},
        )
        cmd = exc._build_sbatch(res, ["echo"])
        assert "--python" not in cmd
        assert "--partition" in cmd

    def test_dependency_in_submit_options(self) -> None:
        exc = SlurmExecutor(mode="dry-run")
        res = JobResources(
            job_name="test",
            submit_options={"dependency": "afterany:123:456"},
        )
        cmd = exc._build_sbatch(res, ["echo"])
        assert "--dependency" in cmd
        assert "afterany:123:456" in cmd


# ═══════════════════════════════════════════════════════════════════════════
# flow.py
# ═══════════════════════════════════════════════════════════════════════════


class TestFlowExtended:
    def test_duplicate_registration_raises(self) -> None:
        f = Flow("test")

        @f.job()
        def task_a() -> str:
            return "ok"

        with pytest.raises(ValueError, match="already registered"):

            @f.job(name="task_a")
            def task_a_dup() -> str:
                return "dup"

    def test_array_job_without_result_raises(self) -> None:
        f = Flow("test")
        with pytest.raises(ValueError, match="must have at least one"):

            @f.array_job()
            def bad_array(x: str) -> str:
                return x

    def test_custom_name(self) -> None:
        f = Flow("test")

        @f.job(name="custom_name")
        def my_func() -> str:
            return "ok"

        assert "custom_name" in f.tasks
        assert "my_func" not in f.tasks

    def test_prefix_none_returns_copies(self) -> None:
        f = Flow("test")

        @f.job()
        def a() -> str:
            return "ok"

        tasks, order = f._prefixed_tasks(None)
        assert "a" in tasks
        assert tasks["a"] is not f.tasks["a"]  # deep copy

    def test_job_config_properties(self) -> None:
        cfg = JobConfig(
            submit_options={
                "partition": "gpu",
                "account": "acc",
                "mail_user": "u@e",
                "mail_type": "FAIL",
                "signal": "B:INT@60",
            }
        )
        assert cfg.partition == "gpu"
        assert cfg.account == "acc"
        assert cfg.mail_user == "u@e"
        assert cfg.mail_type == "FAIL"
        assert cfg.signal == "B:INT@60"
        assert cfg.extra is cfg.submit_options


# ═══════════════════════════════════════════════════════════════════════════
# params.py — extended
# ═══════════════════════════════════════════════════════════════════════════


class TestParamsExtended:
    def test_result_repr_single(self) -> None:
        r = Result(step="foo")
        assert "step=" in repr(r) and "foo" in repr(r)

    def test_result_repr_multi(self) -> None:
        r = Result(steps=["a", "b"])
        assert "steps=" in repr(r)

    def test_param_repr(self) -> None:
        p = Param(help="test", short="-t", namespace="local")
        r = repr(p)
        assert "help=" in r
        assert "short=" in r
        assert "namespace=" in r

    def test_argparse_type_callable_defaults(self) -> None:
        assert argparse_type_callable(str) is str
        assert argparse_type_callable(int) is int
        assert argparse_type_callable(float) is float
        assert argparse_type_callable(Path) is Path

    def test_argparse_type_callable_unknown(self) -> None:
        assert argparse_type_callable(object) is str

    def test_check_type_compatibility_error_message(self) -> None:
        with pytest.raises(TypeError, match="wiring"):
            check_type_compatibility(
                str,
                False,
                str,
                True,
                "up",
                "down",
                "param",
            )

    def test_collect_result_deps(self) -> None:
        def fn(
            item: Annotated[str, Result(step="p")],
            other: str,
        ) -> str:
            return item

        deps = collect_result_deps(fn)
        assert "item" in deps
        assert "other" not in deps

    def test_get_return_type(self) -> None:
        def fn() -> list[str]:
            return []

        assert get_return_type(fn) == list[str]

    def test_merge_resolved_params_dedup(self) -> None:
        p1 = ResolvedParam("x", "t1", str, False, True, None, Param(help="h"))
        p2 = ResolvedParam("x", "t2", str, False, False, "default", Param(help="h"))
        merged = merge_resolved_params([p1, p2])
        globals_ = [p for p in merged if p.namespace == "global"]
        assert len(globals_) == 1
        # required stays True since at least one task requires it
        assert globals_[0].required is True

    def test_resolved_param_local_flag(self) -> None:
        rp = ResolvedParam(
            "chunk_size",
            "convert",
            int,
            False,
            False,
            256,
            Param(namespace="local"),
        )
        assert rp.cli_flag() == "--convert-chunk-size"
        assert rp.dest_name() == "convert_chunk_size"


# ═══════════════════════════════════════════════════════════════════════════
# workflow.py — helpers
# ═══════════════════════════════════════════════════════════════════════════


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


class TestBuildKwargsExtended:
    @staticmethod
    def _spec(func: Any) -> TaskSpec:
        return TaskSpec(
            name="t",
            func=func,
            config=JobConfig(),
            signature=inspect.signature(func),
            result_deps=collect_result_deps(func),
            return_type=get_return_type(func),
        )

    def test_task_local_params(self) -> None:
        def fn(chunk: int = 256) -> None:
            pass

        kw = build_kwargs(
            self._spec(fn),
            {"__task_params__": {"t": {"chunk": 512}}},
            {},
        )
        assert kw["chunk"] == 512

    def test_run_dir_by_name(self) -> None:
        """Parameter named 'run_dir' without RunDir annotation."""

        def fn(run_dir: str) -> None:
            pass

        kw = build_kwargs(self._spec(fn), {"run_dir": "/scratch"}, {})
        assert kw["run_dir"] == Path("/scratch")


# ═══════════════════════════════════════════════════════════════════════════
# workflow.py — dispatch paths
# ═══════════════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════════════
# cli.py — extended command paths
# ═══════════════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════════════
# cache.py — extended verify edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestCacheVerifyExtended:
    def test_verify_path_with_bad_value(self) -> None:
        """Non-path-convertible value with Path return type trusts cache."""
        assert verify_cached_output(None, Path, None) is True

    def test_verify_list_path_with_bad_value(self) -> None:
        assert verify_cached_output(None, list[Path], None) is True

    def test_verify_non_path_type(self) -> None:
        assert verify_cached_output({"key": "val"}, dict, None) is True

    def test_output_hash_deterministic_with_default_str(self) -> None:
        """compute_output_hash uses default=str for non-serializable."""
        h = compute_output_hash(Path("/a/b"))
        assert len(h) == 16


# ═══════════════════════════════════════════════════════════════════════════
# signals.py — edge case
# ═══════════════════════════════════════════════════════════════════════════


class TestSignalsExtended:
    def test_task_interrupted_attributes(self) -> None:
        from reflow.signals import TaskInterrupted

        exc = TaskInterrupted(sig.SIGINT)
        assert exc.signal_number == sig.SIGINT
        assert exc.signal_name == "SIGINT"
        assert "SIGINT" in str(exc)


# ═══════════════════════════════════════════════════════════════════════════
# JobResources
# ═══════════════════════════════════════════════════════════════════════════


class TestJobResourcesProperties:
    def test_backward_compat_extra(self) -> None:
        res = JobResources(
            job_name="test",
            submit_options={"partition": "gpu"},
        )
        assert res.extra["partition"] == "gpu"
        assert res.extra is res.submit_options

    def test_convenience_properties(self) -> None:
        res = JobResources(
            job_name="test",
            submit_options={
                "partition": "gpu",
                "account": "acc",
                "mail_user": "u@e",
                "mail_type": "ALL",
                "signal": "B:INT@30",
            },
        )
        assert res.partition == "gpu"
        assert res.account == "acc"
        assert res.mail_user == "u@e"
        assert res.mail_type == "ALL"
        assert res.signal == "B:INT@30"

    def test_none_properties(self) -> None:
        res = JobResources(job_name="test")
        assert res.partition is None
        assert res.account is None
        assert res.signal is None


# ═══════════════════════════════════════════════════════════════════════════
# Workflow.validate edge cases
# ═══════════════════════════════════════════════════════════════════════════


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
