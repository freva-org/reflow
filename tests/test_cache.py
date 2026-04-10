"""test_cache.py - refactored reflow tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from reflow import (
    TaskState,
)
from reflow.cache import compute_input_hash, compute_identity, compute_output_hash, verify_cached_output
from reflow.stores.sqlite import SqliteStore

# ═══════════════════════════════════════════════════════════════════════════
# _types.py
# ═══════════════════════════════════════════════════════════════════════════


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
