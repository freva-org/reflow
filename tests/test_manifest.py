"""test_manifest.py - refactored reflow tests."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from reflow import (
    Param,
    Run,
    RunState,
    TaskState,
    Workflow,
)
from reflow._types import TaskState as TS
from reflow.manifest import (
    DEFAULT_CODEC,
    CliParamDescription,
    ManifestCodec,
    TaskDescription,
    WorkflowDescription,
    canonical_manifest_dumps,
    manifest_dumps,
    manifest_loads,
)
from reflow.config import _rosetta_stone
from reflow.stores.records import RunRecord, TaskInstanceRecord, TaskSpecRecord

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
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


class TestRosettaStone:
    def test_sbatch_keys(self) -> None:
        assert _rosetta_stone("sbatch", "partition") == "partition"
        assert _rosetta_stone("sbatch", "account") == "account"

    def test_dry_run_falls_back_to_sbatch(self) -> None:
        assert _rosetta_stone("dry-run", "partition") == "partition"

    def test_unknown_key_raises(self) -> None:
        with pytest.raises(KeyError):
            _rosetta_stone("sbatch", "nonexistent_key")