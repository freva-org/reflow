"""Tests for reflow.results -- file-based worker results."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from reflow._types import TaskState
from reflow.manifest import DEFAULT_CODEC
from reflow.results import (
    _result_filename,
    _results_dir,
    ingest_results,
    write_result,
)
from reflow.stores.sqlite import SqliteStore


# --- helpers ---------------------------------------------------------------


@pytest.fixture()
def cache_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect XDG_CACHE_HOME to a temp directory."""
    cache = tmp_path / "cache" / "reflow"
    monkeypatch.setenv("XDG_CACHE_HOME", str(cache))
    return cache


def _make_store(tmp_path: Path) -> SqliteStore:
    """Create and initialise a fresh store with a dummy run."""
    store = SqliteStore(tmp_path / "test.db")
    store.init()
    store.insert_run("r1", "test_graph", "testuser", {})
    return store


def _insert_instances(
    store: SqliteStore,
    task_name: str,
    count: int,
    singleton: bool = False,
) -> list[int]:
    """Insert SUBMITTED instances and return their IDs."""
    ids = []
    if singleton:
        iid = store.insert_task_instance(
            "r1", task_name, None, TaskState.SUBMITTED, {},
        )
        ids.append(iid)
    else:
        for idx in range(count):
            iid = store.insert_task_instance(
                "r1", task_name, idx, TaskState.SUBMITTED, {},
            )
            ids.append(iid)
    return ids


# --- _result_filename ------------------------------------------------------


class TestResultFilename:
    def test_singleton(self) -> None:
        assert _result_filename("prepare", None) == "prepare__singleton.json"

    def test_array_index(self) -> None:
        assert _result_filename("convert", 0) == "convert__0.json"
        assert _result_filename("convert", 42) == "convert__42.json"

    def test_task_name_preserved(self) -> None:
        assert _result_filename("download_source", 7) == "download_source__7.json"


# --- _results_dir ----------------------------------------------------------


class TestResultsDir:
    def test_creates_directory(self, cache_home: Path) -> None:
        d = _results_dir("run-123")
        assert d.is_dir()
        assert d.name == "run-123"
        assert d.parent.name == "results"

    def test_idempotent(self, cache_home: Path) -> None:
        d1 = _results_dir("run-123")
        d2 = _results_dir("run-123")
        assert d1 == d2

    def test_different_runs_different_dirs(self, cache_home: Path) -> None:
        d1 = _results_dir("run-aaa")
        d2 = _results_dir("run-bbb")
        assert d1 != d2


# --- write_result ----------------------------------------------------------


class TestWriteResult:
    def test_success_creates_file(self, cache_home: Path) -> None:
        path = write_result(
            "r1", "convert", 0,
            instance_id=42, state=TaskState.SUCCESS,
            output="a.nc.zarr", output_hash="abc123",
        )
        assert path.exists()
        assert path.name == "convert__0.json"

    def test_success_payload(self, cache_home: Path) -> None:
        write_result(
            "r1", "convert", 3,
            instance_id=10, state=TaskState.SUCCESS,
            output=["a.nc", "b.nc"], output_hash="def456",
        )
        d = _results_dir("r1")
        data = json.loads((d / "convert__3.json").read_text())
        assert data["task_name"] == "convert"
        assert data["array_index"] == 3
        assert data["instance_id"] == 10
        assert data["state"] == "SUCCESS"
        assert data["output_hash"] == "def456"
        decoded = DEFAULT_CODEC.load_value(data["output"])
        assert decoded == ["a.nc", "b.nc"]

    def test_failure_payload(self, cache_home: Path) -> None:
        write_result(
            "r1", "convert", 1,
            instance_id=11, state=TaskState.FAILED,
            error_text="Traceback:\n  File ...\nValueError: boom",
        )
        d = _results_dir("r1")
        data = json.loads((d / "convert__1.json").read_text())
        assert data["state"] == "FAILED"
        assert "ValueError: boom" in data["error_text"]
        assert data["output"] is None

    def test_singleton(self, cache_home: Path) -> None:
        path = write_result(
            "r1", "prepare", None,
            instance_id=1, state=TaskState.SUCCESS,
            output=["x.nc"], output_hash="gh789",
        )
        assert "singleton" in path.name

    def test_no_tmp_file_left(self, cache_home: Path) -> None:
        write_result(
            "r1", "task", 0,
            instance_id=1, state=TaskState.SUCCESS, output="ok",
        )
        d = _results_dir("r1")
        assert list(d.glob("*.tmp")) == []
        assert len(list(d.glob("*.json"))) == 1

    def test_path_output_roundtrips(self, cache_home: Path) -> None:
        """Path objects survive the manifest codec roundtrip."""
        write_result(
            "r1", "download", None,
            instance_id=1, state=TaskState.SUCCESS,
            output=Path("/scratch/data/era5.nc"), output_hash="p1",
        )
        d = _results_dir("r1")
        data = json.loads((d / "download__singleton.json").read_text())
        decoded = DEFAULT_CODEC.load_value(data["output"])
        assert decoded == Path("/scratch/data/era5.nc")

    def test_overwrite_existing(self, cache_home: Path) -> None:
        """Writing the same task+index twice overwrites cleanly."""
        write_result(
            "r1", "t", 0,
            instance_id=1, state=TaskState.FAILED, error_text="first",
        )
        write_result(
            "r1", "t", 0,
            instance_id=1, state=TaskState.SUCCESS, output="ok",
        )
        d = _results_dir("r1")
        data = json.loads((d / "t__0.json").read_text())
        assert data["state"] == "SUCCESS"


# --- ingest_results --------------------------------------------------------


class TestIngestResults:
    def test_ingest_success(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "convert", 3)

        for idx, iid in enumerate(ids):
            write_result(
                "r1", "convert", idx,
                instance_id=iid, state=TaskState.SUCCESS,
                output=f"out_{idx}.zarr", output_hash=f"oh_{idx}",
            )

        n = ingest_results("r1", store)
        assert n == 3

        for idx in range(3):
            row = store.get_task_instance("r1", "convert", idx)
            assert row is not None
            assert row["state"] == "SUCCESS"
            assert row["output"] == f"out_{idx}.zarr"

    def test_ingest_failure(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "task", 1)

        write_result(
            "r1", "task", 0,
            instance_id=ids[0], state=TaskState.FAILED,
            error_text="something broke",
        )

        n = ingest_results("r1", store)
        assert n == 1

        row = store.get_task_instance("r1", "task", 0)
        assert row is not None
        assert row["state"] == "FAILED"
        assert "something broke" in (row["error_text"] or "")

    def test_ingest_mixed(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "convert", 3)

        write_result(
            "r1", "convert", 0,
            instance_id=ids[0], state=TaskState.SUCCESS,
            output="ok_0", output_hash="h0",
        )
        write_result(
            "r1", "convert", 1,
            instance_id=ids[1], state=TaskState.FAILED,
            error_text="OOM",
        )
        write_result(
            "r1", "convert", 2,
            instance_id=ids[2], state=TaskState.SUCCESS,
            output="ok_2", output_hash="h2",
        )

        n = ingest_results("r1", store)
        assert n == 3

        assert store.get_task_instance("r1", "convert", 0)["state"] == "SUCCESS"
        assert store.get_task_instance("r1", "convert", 1)["state"] == "FAILED"
        assert store.get_task_instance("r1", "convert", 2)["state"] == "SUCCESS"

    def test_ingest_singleton(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "prepare", 1, singleton=True)

        write_result(
            "r1", "prepare", None,
            instance_id=ids[0], state=TaskState.SUCCESS,
            output=["a.nc", "b.nc"], output_hash="oh1",
        )

        n = ingest_results("r1", store)
        assert n == 1

        out = store.get_singleton_output("r1", "prepare")
        assert out == ["a.nc", "b.nc"]

    def test_cleanup_after_ingest(self, tmp_path: Path, cache_home: Path) -> None:
        """After all results are ingested the run directory is removed."""
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "t", 2)

        for idx, iid in enumerate(ids):
            write_result(
                "r1", "t", idx,
                instance_id=iid, state=TaskState.SUCCESS, output="ok",
            )

        d = _results_dir("r1")
        assert d.exists()

        ingest_results("r1", store)

        assert not d.exists()

    def test_no_results_returns_zero(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)
        n = ingest_results("r1", store)
        assert n == 0

    def test_malformed_json_skipped(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "t", 1)

        write_result(
            "r1", "t", 0,
            instance_id=ids[0], state=TaskState.SUCCESS, output="ok",
        )

        d = _results_dir("r1")
        (d / "garbage__0.json").write_text("{invalid json", encoding="utf-8")

        n = ingest_results("r1", store)
        assert n == 1

    def test_incomplete_result_skipped(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "t", 1)

        write_result(
            "r1", "t", 0,
            instance_id=ids[0], state=TaskState.SUCCESS, output="ok",
        )

        d = _results_dir("r1")
        (d / "bad__0.json").write_text(
            json.dumps({"state": "SUCCESS"}), encoding="utf-8",
        )

        n = ingest_results("r1", store)
        assert n == 1

    def test_unexpected_state_skipped(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)

        d = _results_dir("r1")
        (d / "weird__0.json").write_text(
            json.dumps({
                "task_name": "weird",
                "array_index": 0,
                "instance_id": 999,
                "state": "PENDING",
                "output": None,
                "output_hash": "",
                "error_text": "",
            }),
            encoding="utf-8",
        )

        n = ingest_results("r1", store)
        assert n == 0

    def test_output_hash_stored(self, tmp_path: Path, cache_home: Path) -> None:
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "t", 1)

        write_result(
            "r1", "t", 0,
            instance_id=ids[0], state=TaskState.SUCCESS,
            output="result", output_hash="abc123",
        )

        ingest_results("r1", store)
        assert store.get_output_hash("r1", "t", 0) == "abc123"

    def test_idempotent_ingest(self, tmp_path: Path, cache_home: Path) -> None:
        """Calling ingest twice doesn't break anything."""
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "t", 1)

        write_result(
            "r1", "t", 0,
            instance_id=ids[0], state=TaskState.SUCCESS, output="ok",
        )

        n1 = ingest_results("r1", store)
        assert n1 == 1

        n2 = ingest_results("r1", store)
        assert n2 == 0

        assert store.get_task_instance("r1", "t", 0)["state"] == "SUCCESS"

    def test_partial_cleanup_keeps_dir(self, tmp_path: Path, cache_home: Path) -> None:
        """If a malformed file remains the directory is NOT removed."""
        store = _make_store(tmp_path)
        ids = _insert_instances(store, "t", 1)

        write_result(
            "r1", "t", 0,
            instance_id=ids[0], state=TaskState.SUCCESS, output="ok",
        )

        d = _results_dir("r1")
        bad = d / "leftover__0.json"
        bad.write_text("{bad", encoding="utf-8")

        ingest_results("r1", store)

        assert d.exists()
        assert bad.exists()
