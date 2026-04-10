"""test_store.py - refactored reflow tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from reflow import (
    Run,
    RunState,
    TaskState,
)
from reflow.flow import TaskSpec
from reflow.stores.sqlite import SqliteStore
from reflow.stores.records import RunRecord, TaskInstanceRecord, TaskSpecRecord

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════


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
